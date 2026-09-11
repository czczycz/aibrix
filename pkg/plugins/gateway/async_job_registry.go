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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	asyncJobRedisKeyPrefix      = "aibrix:gateway:async_job:"
	asyncJobRedisOwnerKeyPrefix = asyncJobRedisKeyPrefix + "owner:"
	asyncJobIDAllocationRetries = 3
)

var (
	// ErrAsyncJobNotFound is returned for both a missing job and a job owned by
	// somebody else. Returning one error for both cases prevents a caller from
	// probing which public job IDs belong to another owner.
	ErrAsyncJobNotFound = errors.New("async job not found")
	// ErrAsyncJobAlreadyExists lets a registry retry the vanishingly unlikely
	// collision from a generated public ID without allowing a store to overwrite
	// an existing job's routing record.
	ErrAsyncJobAlreadyExists = errors.New("async job already exists")
	// ErrInvalidAsyncJobRecord reports a record that cannot safely route a job.
	ErrInvalidAsyncJobRecord = errors.New("invalid async job record")
)

// AsyncJobBackendPod is the stable identity of the pod holding a job's state.
// UID is required so that a replacement pod with the same name is not treated
// as the original backend after a reschedule.
type AsyncJobBackendPod struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

// AsyncJobRecord is durable routing metadata for one asynchronous backend job.
// It deliberately does not carry a lifecycle status: the backend remains the
// source of truth for status and results, while this registry only owns public
// identity, authorization, expiry, and the backend pin.
type AsyncJobRecord struct {
	PublicJobID  string             `json:"public_job_id"`
	JobType      string             `json:"job_type"`
	Owner        string             `json:"owner"`
	Model        string             `json:"model"`
	BackendJobID string             `json:"backend_job_id"`
	BackendPod   AsyncJobBackendPod `json:"backend_pod"`
	CreatedAt    time.Time          `json:"created_at"`
	ExpiresAt    time.Time          `json:"expires_at"`
}

// AsyncJobRegistration is the backend-derived data needed to register a job
// after a successful create response has already been received. Register never
// creates or calls a backend job itself.
type AsyncJobRegistration struct {
	JobType      string
	Owner        string
	Model        string
	BackendJobID string
	BackendPod   AsyncJobBackendPod
	ExpiresAt    time.Time
}

// AsyncJobListOptions narrows an owner's job catalog. Empty fields do not
// filter, which lets future asynchronous APIs reuse the same registry.
type AsyncJobListOptions struct {
	JobType string
	Model   string
}

// AsyncJobStore persists routing records. Store implementations must not
// silently overwrite a public ID: Register relies on ErrAsyncJobAlreadyExists
// to preserve the one-to-one public-to-backend mapping.
type AsyncJobStore interface {
	Create(context.Context, AsyncJobRecord) error
	Get(context.Context, string) (AsyncJobRecord, error)
	List(context.Context, string) ([]AsyncJobRecord, error)
	Delete(context.Context, AsyncJobRecord) error
}

// AsyncJobRegistry owns public IDs, ownership checks, and expiry. API-specific
// handlers remain responsible for authentication plus request-path and response
// ID rewriting before they call into this registry.
type AsyncJobRegistry struct {
	store AsyncJobStore
	now   func() time.Time
	newID func() string
}

// NewAsyncJobRegistry creates a registry over store. The in-memory store is
// suitable for tests and local deployments; Redis shares records across gateway
// replicas.
func NewAsyncJobRegistry(store AsyncJobStore) *AsyncJobRegistry {
	if store == nil {
		panic("async job registry requires a store")
	}
	return &AsyncJobRegistry{
		store: store,
		now:   func() time.Time { return time.Now().UTC() },
		newID: uuid.NewString,
	}
}

// Register creates an opaque AIBrix public ID and stores the backend routing
// record. It must be called only after a successful backend create response, so
// failed requests cannot leave pending jobs in the catalog.
func (r *AsyncJobRegistry) Register(ctx context.Context, registration AsyncJobRegistration) (AsyncJobRecord, error) {
	if err := r.validateRegistration(registration); err != nil {
		return AsyncJobRecord{}, err
	}

	for attempt := 0; attempt < asyncJobIDAllocationRetries; attempt++ {
		record := AsyncJobRecord{
			PublicJobID:  r.newID(),
			JobType:      registration.JobType,
			Owner:        registration.Owner,
			Model:        registration.Model,
			BackendJobID: registration.BackendJobID,
			BackendPod:   registration.BackendPod,
			CreatedAt:    r.now(),
			ExpiresAt:    registration.ExpiresAt.UTC(),
		}
		if err := r.store.Create(ctx, record); err != nil {
			if errors.Is(err, ErrAsyncJobAlreadyExists) {
				continue
			}
			return AsyncJobRecord{}, err
		}
		return record, nil
	}
	return AsyncJobRecord{}, fmt.Errorf("could not allocate a unique async job ID: %w", ErrAsyncJobAlreadyExists)
}

// Get returns a job only when it belongs to owner. A missing and an
// unauthorized job both intentionally return ErrAsyncJobNotFound.
func (r *AsyncJobRegistry) Get(ctx context.Context, publicJobID, owner string) (AsyncJobRecord, error) {
	if publicJobID == "" || owner == "" {
		return AsyncJobRecord{}, ErrAsyncJobNotFound
	}
	record, err := r.store.Get(ctx, publicJobID)
	if err != nil {
		if errors.Is(err, ErrAsyncJobNotFound) {
			return AsyncJobRecord{}, ErrAsyncJobNotFound
		}
		return AsyncJobRecord{}, err
	}
	if record.Owner != owner || r.expired(record) {
		if r.expired(record) {
			_ = r.store.Delete(ctx, record)
		}
		return AsyncJobRecord{}, ErrAsyncJobNotFound
	}
	return record, nil
}

// List returns the unexpired jobs visible to owner, sorted by creation time.
// The store is the public catalog; callers may query backends only to enrich
// these records with current status, never to discover additional public IDs.
func (r *AsyncJobRegistry) List(ctx context.Context, owner string, options AsyncJobListOptions) ([]AsyncJobRecord, error) {
	if owner == "" {
		return nil, ErrAsyncJobNotFound
	}
	records, err := r.store.List(ctx, owner)
	if err != nil {
		return nil, err
	}

	jobs := make([]AsyncJobRecord, 0, len(records))
	for _, record := range records {
		if record.Owner != owner {
			continue
		}
		if r.expired(record) {
			_ = r.store.Delete(ctx, record)
			continue
		}
		if options.JobType != "" && record.JobType != options.JobType {
			continue
		}
		if options.Model != "" && record.Model != options.Model {
			continue
		}
		jobs = append(jobs, record)
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].PublicJobID < jobs[j].PublicJobID
		}
		return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
	})
	return jobs, nil
}

// Delete removes an owned, unexpired job from the catalog. Callers should only
// invoke it after the backend accepts cancellation/deletion (or returns 404),
// so retryable backend failures retain the route for another attempt.
func (r *AsyncJobRegistry) Delete(ctx context.Context, publicJobID, owner string) error {
	record, err := r.Get(ctx, publicJobID, owner)
	if err != nil {
		return err
	}
	return r.store.Delete(ctx, record)
}

func (r *AsyncJobRegistry) validateRegistration(registration AsyncJobRegistration) error {
	if registration.JobType == "" || registration.Owner == "" || registration.Model == "" || registration.BackendJobID == "" ||
		registration.BackendPod.Namespace == "" || registration.BackendPod.Name == "" || registration.BackendPod.UID == "" ||
		registration.ExpiresAt.IsZero() || !registration.ExpiresAt.After(r.now()) {
		return fmt.Errorf("%w: job type, owner, model, backend job ID, backend pod identity, and a future expiry are required", ErrInvalidAsyncJobRecord)
	}
	return nil
}

func (r *AsyncJobRegistry) expired(record AsyncJobRecord) bool {
	return !record.ExpiresAt.After(r.now())
}

// InMemoryAsyncJobStore is a process-local store for tests and single-replica
// local deployments. It keeps the same no-overwrite contract as Redis.
type InMemoryAsyncJobStore struct {
	mu   sync.RWMutex
	jobs map[string]AsyncJobRecord
}

func NewInMemoryAsyncJobStore() *InMemoryAsyncJobStore {
	return &InMemoryAsyncJobStore{jobs: make(map[string]AsyncJobRecord)}
}

func (s *InMemoryAsyncJobStore) Create(_ context.Context, record AsyncJobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[record.PublicJobID]; exists {
		return ErrAsyncJobAlreadyExists
	}
	s.jobs[record.PublicJobID] = record
	return nil
}

func (s *InMemoryAsyncJobStore) Get(_ context.Context, publicJobID string) (AsyncJobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, exists := s.jobs[publicJobID]
	if !exists {
		return AsyncJobRecord{}, ErrAsyncJobNotFound
	}
	return record, nil
}

func (s *InMemoryAsyncJobStore) List(_ context.Context, owner string) ([]AsyncJobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	records := make([]AsyncJobRecord, 0)
	for _, record := range s.jobs {
		if record.Owner == owner {
			records = append(records, record)
		}
	}
	return records, nil
}

func (s *InMemoryAsyncJobStore) Delete(_ context.Context, record AsyncJobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, record.PublicJobID)
	return nil
}

// RedisAsyncJobStore keeps records durable across gateway replicas. The owner
// index contains only a SHA-256 digest, so owner names are not exposed in Redis
// keys while List can still find that owner's public catalog efficiently.
type RedisAsyncJobStore struct {
	client redis.UniversalClient
}

func NewRedisAsyncJobStore(client redis.UniversalClient) *RedisAsyncJobStore {
	if client == nil {
		panic("redis async job store requires a client")
	}
	return &RedisAsyncJobStore{client: client}
}

func (s *RedisAsyncJobStore) Create(ctx context.Context, record AsyncJobRecord) error {
	ttl := time.Until(record.ExpiresAt)
	if ttl <= 0 {
		return fmt.Errorf("%w: expiry must be in the future", ErrInvalidAsyncJobRecord)
	}
	payload, err := sonic.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal async job record: %w", err)
	}
	created, err := s.client.SetNX(ctx, asyncJobRedisKey(record.PublicJobID), payload, ttl).Result()
	if err != nil {
		return fmt.Errorf("store async job record: %w", err)
	}
	if !created {
		return ErrAsyncJobAlreadyExists
	}
	if err := s.client.ZAdd(ctx, asyncJobRedisOwnerKey(record.Owner), redis.Z{Score: float64(record.ExpiresAt.UnixMilli()), Member: record.PublicJobID}).Err(); err != nil {
		// The record is unusable without its owner catalog entry. Best-effort
		// rollback avoids leaving an unlistable record after a transient error.
		_ = s.client.Del(ctx, asyncJobRedisKey(record.PublicJobID)).Err()
		return fmt.Errorf("index async job record: %w", err)
	}
	return nil
}

func (s *RedisAsyncJobStore) Get(ctx context.Context, publicJobID string) (AsyncJobRecord, error) {
	payload, err := s.client.Get(ctx, asyncJobRedisKey(publicJobID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return AsyncJobRecord{}, ErrAsyncJobNotFound
		}
		return AsyncJobRecord{}, fmt.Errorf("load async job record: %w", err)
	}
	var record AsyncJobRecord
	if err := sonic.Unmarshal(payload, &record); err != nil {
		return AsyncJobRecord{}, fmt.Errorf("unmarshal async job record: %w", err)
	}
	return record, nil
}

func (s *RedisAsyncJobStore) List(ctx context.Context, owner string) ([]AsyncJobRecord, error) {
	ownerKey := asyncJobRedisOwnerKey(owner)
	publicJobIDs, err := s.client.ZRange(ctx, ownerKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("list async job IDs: %w", err)
	}
	if len(publicJobIDs) == 0 {
		return []AsyncJobRecord{}, nil
	}

	payloads, err := s.client.MGet(ctx, asyncJobRedisKeys(publicJobIDs)...).Result()
	if err != nil {
		return nil, fmt.Errorf("load async job records: %w", err)
	}
	records := make([]AsyncJobRecord, 0, len(payloads))
	for i, payload := range payloads {
		raw, ok := payload.(string)
		if !ok {
			// The record key can expire before this index entry. Remove the stale
			// member so repeated List calls do not keep issuing a useless MGET.
			if err := s.client.ZRem(ctx, ownerKey, publicJobIDs[i]).Err(); err != nil {
				return nil, fmt.Errorf("remove stale async job ID: %w", err)
			}
			continue
		}
		var record AsyncJobRecord
		if err := sonic.UnmarshalString(raw, &record); err != nil {
			return nil, fmt.Errorf("unmarshal async job record: %w", err)
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *RedisAsyncJobStore) Delete(ctx context.Context, record AsyncJobRecord) error {
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, asyncJobRedisKey(record.PublicJobID))
		pipe.ZRem(ctx, asyncJobRedisOwnerKey(record.Owner), record.PublicJobID)
		return nil
	})
	if err != nil {
		return fmt.Errorf("delete async job record: %w", err)
	}
	return nil
}

func asyncJobRedisKey(publicJobID string) string {
	return asyncJobRedisKeyPrefix + publicJobID
}

func asyncJobRedisOwnerKey(owner string) string {
	digest := sha256.Sum256([]byte(owner))
	return asyncJobRedisOwnerKeyPrefix + hex.EncodeToString(digest[:])
}

func asyncJobRedisKeys(publicJobIDs []string) []string {
	keys := make([]string, len(publicJobIDs))
	for i, publicJobID := range publicJobIDs {
		keys[i] = asyncJobRedisKey(publicJobID)
	}
	return keys
}
