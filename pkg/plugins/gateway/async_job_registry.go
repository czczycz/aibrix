/*
Copyright 2026 The Aibrix Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gateway

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// AsyncJobTypeVideo is the job type for video generation jobs. Job types are
// free-form strings so a new asynchronous API can register here without
// touching the registry; this constant only names the one that exists today.
const AsyncJobTypeVideo = "video"

// asyncJobPublicIDPrefix marks a public ID as minted by this gateway. The
// suffix is a UUID: the public ID must carry no backend information, since it
// is the only identifier a client ever sees.
const asyncJobPublicIDPrefix = "aibrix_job_"

const (
	// asyncJobRedisKeyPrefix namespaces per-owner job records in the shared
	// Redis keyspace. Deliberately distinct from videoJobRedisKeyPrefix: the
	// video routing cache holds unauthenticated video_id -> pod mappings.
	asyncJobRedisKeyPrefix = "aibrix:gateway:async_job:"
)

var (
	// ErrAsyncJobNotFound is returned for a job that does not exist, has
	// expired, or belongs to another owner. Those cases are deliberately
	// indistinguishable: telling them apart would let a caller probe whether
	// someone else's public ID exists.
	ErrAsyncJobNotFound = errors.New("async job not found")
	// ErrAsyncJobExists is returned when a public ID is already taken. The
	// registry mints a fresh UUID per registration, so this means a store-level
	// collision, never a legitimate re-registration.
	ErrAsyncJobExists = errors.New("async job already registered")
	// ErrAsyncJobInvalidRegistration is returned when registration data is
	// incomplete. Every field it guards is needed to route a follow-up request
	// back to the pod holding the job.
	ErrAsyncJobInvalidRegistration = errors.New("invalid async job registration")
)

func asyncJobRedisOwnerPrefix(owner string) string {
	// Owners are encoded rather than hashed: this keeps their identity in the
	// key while preventing SCAN's glob syntax from interpreting owner bytes.
	return asyncJobRedisKeyPrefix + base64.RawURLEncoding.EncodeToString([]byte(owner)) + ":"
}

func asyncJobRedisKey(owner, publicID string) string {
	return asyncJobRedisOwnerPrefix(owner) + publicID
}

// AsyncJobRecord is the durable mapping from an opaque AIBrix public job ID to
// an already-created backend job and the pod that owns it. It holds no job
// status: the backend pod stays the single source of truth for how a job is
// progressing and for its results, so there is nothing here to keep in sync.
// JSON tags are the persisted representation shared across gateway replicas.
type AsyncJobRecord struct {
	// PublicID is the only identifier a client sees; it is minted here and
	// never derived from BackendJobID.
	PublicID string `json:"public_id"`
	JobType  string `json:"job_type"`
	Owner    string `json:"owner"`
	Model    string `json:"model"`
	// BackendJobID is the id the backend pod assigned, used when a follow-up
	// request is proxied to that pod.
	BackendJobID string `json:"backend_job_id"`
	PodNamespace string `json:"pod_namespace"`
	PodName      string `json:"pod_name"`
	// PodUID disambiguates a rescheduled pod: a name can be reused by a
	// different pod, and routing to that one would find no such job.
	PodUID    string    `json:"pod_uid"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// expired reports whether the record is no longer valid as of now.
func (r AsyncJobRecord) expired(now time.Time) bool {
	return !now.Before(r.ExpiresAt)
}

// AsyncJobRegistration is the backend-derived data needed to register a job.
// Every field comes from a backend job that has already been created: the
// registry itself makes no HTTP or backend calls, it only records the outcome.
type AsyncJobRegistration struct {
	JobType      string
	Owner        string
	Model        string
	BackendJobID string
	PodNamespace string
	PodName      string
	PodUID       string
	// ExpiresAt must be in the future; it becomes the record's storage TTL.
	ExpiresAt time.Time
}

// validate rejects registration data that could not route a follow-up request.
func (r AsyncJobRegistration) validate(now time.Time) error {
	missing := make([]string, 0, 7)
	for _, field := range []struct {
		name  string
		value string
	}{
		{"job type", r.JobType},
		{"owner", r.Owner},
		{"model", r.Model},
		{"backend job id", r.BackendJobID},
		{"pod namespace", r.PodNamespace},
		{"pod name", r.PodName},
		{"pod uid", r.PodUID},
	} {
		if field.value == "" {
			missing = append(missing, field.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: missing %s", ErrAsyncJobInvalidRegistration, strings.Join(missing, ", "))
	}
	if r.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: missing expiry", ErrAsyncJobInvalidRegistration)
	}
	if !r.ExpiresAt.After(now) {
		return fmt.Errorf("%w: expiry %s is not in the future", ErrAsyncJobInvalidRegistration, r.ExpiresAt)
	}
	return nil
}

// AsyncJobFilter narrows List. A zero value matches every record the owner has;
// a non-empty field is an exact match.
type AsyncJobFilter struct {
	JobType string
	Model   string
}

func (f AsyncJobFilter) matches(rec AsyncJobRecord) bool {
	if f.JobType != "" && f.JobType != rec.JobType {
		return false
	}
	if f.Model != "" && f.Model != rec.Model {
		return false
	}
	return true
}

// AsyncJobStore is the storage behind AsyncJobRegistry. Records are keyed by
// owner and public ID, so ListByOwner scans only that owner's key prefix rather
// than keeping a separate catalog. Implementations enforce storage-level
// contracts only; expiry semantics belong to the registry.
type AsyncJobStore interface {
	// Put stores rec under its owner-specific key. It returns ErrAsyncJobExists
	// rather than overwriting an existing public ID for that owner.
	Put(ctx context.Context, rec AsyncJobRecord) error
	// Get returns owner's record for publicID, or ErrAsyncJobNotFound. It does not
	// interpret expiry: a record that outlived its expiry may still be
	// returned, and the registry reaps it.
	Get(ctx context.Context, owner, publicID string) (AsyncJobRecord, error)
	// ListByOwner returns owner's records in no particular order.
	ListByOwner(ctx context.Context, owner string) ([]AsyncJobRecord, error)
	// Delete removes rec only when the stored value still equals rec. This
	// compare-and-delete prevents a delayed delete from erasing a job that was
	// registered under the same key after the original record expired.
	Delete(ctx context.Context, rec AsyncJobRecord) error
}

// AsyncJobRegistry maps opaque public job IDs to backend jobs, scoped by
// owner. It is a bookkeeping layer only: it never talks to a backend, never
// polls job status, and holds no lifecycle state machine.
type AsyncJobRegistry struct {
	store AsyncJobStore
}

// NewAsyncJobRegistry returns a registry backed by store.
func NewAsyncJobRegistry(store AsyncJobStore) *AsyncJobRegistry {
	return &AsyncJobRegistry{store: store}
}

// Register records an already-created backend job under a freshly minted
// public ID and returns the stored record. It is called after backend
// creation has succeeded, so a failure here means the job exists on the pod
// but is unreachable through the gateway -- never a partially created job.
func (r *AsyncJobRegistry) Register(ctx context.Context, reg AsyncJobRegistration) (AsyncJobRecord, error) {
	now := time.Now()
	if err := reg.validate(now); err != nil {
		return AsyncJobRecord{}, err
	}

	rec := AsyncJobRecord{
		PublicID:     asyncJobPublicIDPrefix + strings.ReplaceAll(uuid.NewString(), "-", ""),
		JobType:      reg.JobType,
		Owner:        reg.Owner,
		Model:        reg.Model,
		BackendJobID: reg.BackendJobID,
		PodNamespace: reg.PodNamespace,
		PodName:      reg.PodName,
		PodUID:       reg.PodUID,
		CreatedAt:    now,
		ExpiresAt:    reg.ExpiresAt,
	}
	if err := r.store.Put(ctx, rec); err != nil {
		return AsyncJobRecord{}, err
	}
	return rec, nil
}

// Get returns owner's job. A job belonging to somebody else, an expired job
// and an unknown public ID all yield ErrAsyncJobNotFound.
func (r *AsyncJobRegistry) Get(ctx context.Context, owner, publicID string) (AsyncJobRecord, error) {
	if owner == "" || publicID == "" {
		return AsyncJobRecord{}, ErrAsyncJobNotFound
	}
	rec, err := r.store.Get(ctx, owner, publicID)
	if err != nil {
		return AsyncJobRecord{}, err
	}
	if rec.Owner != owner {
		return AsyncJobRecord{}, ErrAsyncJobNotFound
	}
	if rec.expired(time.Now()) {
		r.reap(ctx, rec)
		return AsyncJobRecord{}, ErrAsyncJobNotFound
	}
	return rec, nil
}

// List returns owner's unexpired jobs matching filter, oldest first with the
// public ID breaking created-at ties so pagination over it is stable.
// Expired records met along the way are reaped.
func (r *AsyncJobRegistry) List(ctx context.Context, owner string, filter AsyncJobFilter) ([]AsyncJobRecord, error) {
	if owner == "" {
		return nil, nil
	}
	records, err := r.store.ListByOwner(ctx, owner)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	matched := make([]AsyncJobRecord, 0, len(records))
	for _, rec := range records {
		if rec.Owner != owner {
			continue
		}
		if rec.expired(now) {
			r.reap(ctx, rec)
			continue
		}
		if filter.matches(rec) {
			matched = append(matched, rec)
		}
	}

	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].CreatedAt.Before(matched[j].CreatedAt)
		}
		return matched[i].PublicID < matched[j].PublicID
	})
	return matched, nil
}

// Delete forgets owner's job. Ownership and expiry are confirmed first, so a
// caller can neither delete nor probe another owner's job.
func (r *AsyncJobRegistry) Delete(ctx context.Context, owner, publicID string) error {
	rec, err := r.Get(ctx, owner, publicID)
	if err != nil {
		return err
	}
	return r.store.Delete(ctx, rec)
}

// reap drops an expired record and its catalog entry. Failing to reap only
// leaves garbage behind -- the record is already invisible to callers -- so it
// is logged rather than surfaced.
func (r *AsyncJobRegistry) reap(ctx context.Context, rec AsyncJobRecord) {
	_ = r.store.Delete(ctx, rec)
}

// InMemoryAsyncJobStore is a process-local AsyncJobStore for tests and for
// single-replica or local runs. Records live only as long as the process, so a
// multi-replica gateway wants RedisAsyncJobStore instead.
type InMemoryAsyncJobStore struct {
	mu      sync.RWMutex
	records map[string]AsyncJobRecord
}

// NewInMemoryAsyncJobStore returns an empty in-memory store.
func NewInMemoryAsyncJobStore() *InMemoryAsyncJobStore {
	return &InMemoryAsyncJobStore{
		records: make(map[string]AsyncJobRecord),
	}
}

func (s *InMemoryAsyncJobStore) Put(ctx context.Context, rec AsyncJobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := rec.Owner + "\x00" + rec.PublicID
	if _, exists := s.records[key]; exists {
		return ErrAsyncJobExists
	}
	s.records[key] = rec
	return nil
}

func (s *InMemoryAsyncJobStore) Get(ctx context.Context, owner, publicID string) (AsyncJobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.records[owner+"\x00"+publicID]
	if !ok {
		return AsyncJobRecord{}, ErrAsyncJobNotFound
	}
	return rec, nil
}

func (s *InMemoryAsyncJobStore) ListByOwner(ctx context.Context, owner string) ([]AsyncJobRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]AsyncJobRecord, 0)
	for _, rec := range s.records {
		if rec.Owner == owner {
			records = append(records, rec)
		}
	}
	return records, nil
}

func (s *InMemoryAsyncJobStore) Delete(ctx context.Context, rec AsyncJobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := rec.Owner + "\x00" + rec.PublicID
	stored, exists := s.records[key]
	if !exists || stored != rec {
		return ErrAsyncJobNotFound
	}
	delete(s.records, key)
	return nil
}

// RedisAsyncJobStore persists records in Redis so every gateway replica sees
// the same jobs. The owner is part of each record key, so ListByOwner scans
// only that owner's prefix and Redis can expire each record independently.
type RedisAsyncJobStore struct {
	client *redis.Client
}

// NewRedisAsyncJobStore returns a store backed by client.
func NewRedisAsyncJobStore(client *redis.Client) *RedisAsyncJobStore {
	return &RedisAsyncJobStore{client: client}
}

// asyncJobPutScript stores the record only when its owner-specific key is free.
// KEYS: record key. ARGV: payload, ttl in milliseconds.
var asyncJobPutScript = redis.NewScript(`
return redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) and 1 or 0
`)

// asyncJobDeleteScript deletes only the exact record returned by a preceding
// Get. A delayed delete must not erase a record registered under the same key
// after the old record expired. KEYS: record key. ARGV: expected payload.
var asyncJobDeleteScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return 0
end
return redis.call("DEL", KEYS[1])
`)

// asyncJobRedisPX converts a remaining lifetime into the millisecond argument
// of Redis' PX option, rounding up. Truncating would end the key's TTL before
// ExpiresAt -- by up to a millisecond, and by the whole lifetime for a
// sub-millisecond one, where a PX of 0 is rejected outright. Redis would then
// drop a record the registry still considers valid, and Get would answer "not
// found" for a job the gateway is holding. Rounding up only keeps the key for
// less than a millisecond too long, which the registry's own timestamp check
// hides from callers anyway. ttl is positive: Put refuses anything else.
func asyncJobRedisPX(ttl time.Duration) int64 {
	return int64((ttl + time.Millisecond - 1) / time.Millisecond)
}

func (s *RedisAsyncJobStore) Put(ctx context.Context, rec AsyncJobRecord) error {
	ttl := time.Until(rec.ExpiresAt)
	if ttl <= 0 {
		// Would be reaped by Redis instantly; refuse rather than store a key
		// whose absence the registry cannot tell from a successful Put.
		return fmt.Errorf("%w: expiry %s is not in the future", ErrAsyncJobInvalidRegistration, rec.ExpiresAt)
	}
	payload, err := sonic.Marshal(rec)
	if err != nil {
		return fmt.Errorf("failed to marshal async job record %s: %w", rec.PublicID, err)
	}

	stored, err := asyncJobPutScript.Run(ctx, s.client,
		[]string{asyncJobRedisKey(rec.Owner, rec.PublicID)}, string(payload), asyncJobRedisPX(ttl)).Int64()
	if err != nil {
		return fmt.Errorf("failed to persist async job record %s: %w", rec.PublicID, err)
	}
	if stored == 0 {
		// The script uses SET NX: the public ID must not be silently rebound
		// to a different backend job.
		return ErrAsyncJobExists
	}
	return nil
}

func (s *RedisAsyncJobStore) Get(ctx context.Context, owner, publicID string) (AsyncJobRecord, error) {
	val, err := s.client.Get(ctx, asyncJobRedisKey(owner, publicID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return AsyncJobRecord{}, ErrAsyncJobNotFound
		}
		return AsyncJobRecord{}, fmt.Errorf("failed to look up async job record %s: %w", publicID, err)
	}
	var rec AsyncJobRecord
	if err := sonic.UnmarshalString(val, &rec); err != nil {
		return AsyncJobRecord{}, fmt.Errorf("failed to unmarshal async job record %s: %w", publicID, err)
	}
	return rec, nil
}

func (s *RedisAsyncJobStore) ListByOwner(ctx context.Context, owner string) ([]AsyncJobRecord, error) {
	var cursor uint64
	records := make([]AsyncJobRecord, 0)
	seen := make(map[string]struct{})
	pattern := asyncJobRedisOwnerPrefix(owner) + "*"
	for {
		keys, next, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan async jobs for owner: %w", err)
		}
		for _, key := range keys {
			// SCAN is not a snapshot and may return a key more than once while
			// the keyspace changes. A List response must not duplicate a job.
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			raw, err := s.client.Get(ctx, key).Result()
			if errors.Is(err, redis.Nil) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("read async job record: %w", err)
			}
			var rec AsyncJobRecord
			if err := sonic.UnmarshalString(raw, &rec); err != nil {
				return nil, fmt.Errorf("unmarshal async job record %s: %w", key, err)
			}
			if rec.Owner == owner {
				records = append(records, rec)
			}
		}
		cursor = next
		if cursor == 0 {
			return records, nil
		}
	}
}

func (s *RedisAsyncJobStore) Delete(ctx context.Context, rec AsyncJobRecord) error {
	payload, err := sonic.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal async job record %s: %w", rec.PublicID, err)
	}
	removed, err := asyncJobDeleteScript.Run(ctx, s.client,
		[]string{asyncJobRedisKey(rec.Owner, rec.PublicID)}, string(payload)).Int64()
	if err != nil {
		return fmt.Errorf("failed to delete async job record %s: %w", rec.PublicID, err)
	}
	if removed == 0 {
		return ErrAsyncJobNotFound
	}
	return nil
}
