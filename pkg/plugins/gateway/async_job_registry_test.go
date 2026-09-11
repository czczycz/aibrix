/*
Copyright 2026 The Aibrix Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validAsyncJobRegistration() AsyncJobRegistration {
	return AsyncJobRegistration{
		JobType: AsyncJobTypeVideo, Owner: "owner-a", Model: "wan2.1",
		BackendJobID: "video_gen_abc", PodNamespace: "ns", PodName: "pod", PodUID: "uid",
		ExpiresAt: time.Now().Add(time.Hour),
	}
}

func newTestAsyncJobRedisStore(t *testing.T) (*RedisAsyncJobStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisAsyncJobStore(client), mr
}

func TestAsyncJobRegistryRegisterValidatesAndMintsOpaqueID(t *testing.T) {
	registry := NewAsyncJobRegistry(NewInMemoryAsyncJobStore())
	bad := validAsyncJobRegistration()
	bad.PodUID = ""
	_, err := registry.Register(context.Background(), bad)
	assert.ErrorIs(t, err, ErrAsyncJobInvalidRegistration)

	record, err := registry.Register(context.Background(), validAsyncJobRegistration())
	require.NoError(t, err)
	assert.Contains(t, record.PublicID, asyncJobPublicIDPrefix)
	assert.NotContains(t, record.PublicID, record.BackendJobID)
	assert.False(t, record.CreatedAt.IsZero())
}

func TestAsyncJobRegistryOwnerIsolationAndList(t *testing.T) {
	ctx := context.Background()
	registry := NewAsyncJobRegistry(NewInMemoryAsyncJobStore())
	record, err := registry.Register(ctx, validAsyncJobRegistration())
	require.NoError(t, err)

	_, err = registry.Get(ctx, "owner-b", record.PublicID)
	assert.ErrorIs(t, err, ErrAsyncJobNotFound)
	assert.ErrorIs(t, registry.Delete(ctx, "owner-b", record.PublicID), ErrAsyncJobNotFound)
	jobs, err := registry.List(ctx, "owner-b", AsyncJobFilter{})
	require.NoError(t, err)
	assert.Empty(t, jobs)

	jobs, err = registry.List(ctx, "owner-a", AsyncJobFilter{JobType: AsyncJobTypeVideo, Model: "wan2.1"})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, record.PublicID, jobs[0].PublicID)
}

func TestAsyncJobRegistryReapsExpiredRecord(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryAsyncJobStore()
	registry := NewAsyncJobRegistry(store)
	record := AsyncJobRecord{PublicID: "job", JobType: AsyncJobTypeVideo, Owner: "owner-a", Model: "m", BackendJobID: "b", PodNamespace: "ns", PodName: "pod", PodUID: "uid", ExpiresAt: time.Now().Add(-time.Second)}
	require.NoError(t, store.Put(ctx, record))
	_, err := registry.Get(ctx, record.Owner, record.PublicID)
	assert.ErrorIs(t, err, ErrAsyncJobNotFound)
	_, err = store.Get(ctx, record.Owner, record.PublicID)
	assert.ErrorIs(t, err, ErrAsyncJobNotFound)
}

func TestAsyncJobRedisKeysEncodeOwnerWithoutHashing(t *testing.T) {
	key := asyncJobRedisKey("team/*?", "job-1")
	assert.Equal(t, "aibrix:gateway:async_job:dGVhbS8qPw:job-1", key)
	assert.Equal(t, "aibrix:gateway:async_job:dGVhbS8qPw:*", asyncJobRedisOwnerPrefix("team/*?")+"*")
}

func TestRedisAsyncJobStoreCrossReplicaListAndDelete(t *testing.T) {
	ctx := context.Background()
	store, mr := newTestAsyncJobRedisStore(t)
	registry := NewAsyncJobRegistry(store)
	record, err := registry.Register(ctx, validAsyncJobRegistration())
	require.NoError(t, err)

	otherClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = otherClient.Close() })
	other := NewAsyncJobRegistry(NewRedisAsyncJobStore(otherClient))
	got, err := other.Get(ctx, record.Owner, record.PublicID)
	require.NoError(t, err)
	assert.Equal(t, record.BackendJobID, got.BackendJobID)
	jobs, err := other.List(ctx, record.Owner, AsyncJobFilter{})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.NoError(t, other.Delete(ctx, record.Owner, record.PublicID))
	_, err = registry.Get(ctx, record.Owner, record.PublicID)
	assert.ErrorIs(t, err, ErrAsyncJobNotFound)
}

func TestRedisAsyncJobStoreListOnlyScansOwnerPrefix(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAsyncJobRedisStore(t)
	ownerA := AsyncJobRecord{PublicID: "a", JobType: AsyncJobTypeVideo, Owner: "a*", Model: "m", BackendJobID: "a", PodNamespace: "ns", PodName: "p", PodUID: "u", ExpiresAt: time.Now().Add(time.Hour)}
	ownerB := ownerA
	ownerB.PublicID, ownerB.Owner = "b", "abc"
	require.NoError(t, store.Put(ctx, ownerA))
	require.NoError(t, store.Put(ctx, ownerB))
	jobs, err := store.ListByOwner(ctx, ownerA.Owner)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, ownerA.PublicID, jobs[0].PublicID)
}

func TestRedisAsyncJobStoreExpiryNeedsNoCatalogCleanup(t *testing.T) {
	ctx := context.Background()
	store, mr := newTestAsyncJobRedisStore(t)
	registry := NewAsyncJobRegistry(store)
	short := validAsyncJobRegistration()
	short.ExpiresAt = time.Now().Add(time.Minute)
	shortRecord, err := registry.Register(ctx, short)
	require.NoError(t, err)
	long := validAsyncJobRegistration()
	long.BackendJobID = "long"
	long.ExpiresAt = time.Now().Add(time.Hour)
	longRecord, err := registry.Register(ctx, long)
	require.NoError(t, err)
	mr.FastForward(2 * time.Minute)
	_, err = registry.Get(ctx, short.Owner, shortRecord.PublicID)
	assert.ErrorIs(t, err, ErrAsyncJobNotFound)
	jobs, err := registry.List(ctx, long.Owner, AsyncJobFilter{})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, longRecord.PublicID, jobs[0].PublicID)
}

func TestRedisAsyncJobStoreDeleteDoesNotEraseRecreatedRecord(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAsyncJobRedisStore(t)
	old := AsyncJobRecord{PublicID: "job", JobType: AsyncJobTypeVideo, Owner: "owner-a", Model: "m", BackendJobID: "old", PodNamespace: "ns", PodName: "p", PodUID: "u", ExpiresAt: time.Now().Add(time.Hour)}
	require.NoError(t, store.Put(ctx, old))
	// Simulate the old key expiring before an already-authorized Delete reaches Redis.
	require.NoError(t, store.client.Del(ctx, asyncJobRedisKey(old.Owner, old.PublicID)).Err())
	newRecord := old
	newRecord.BackendJobID = "new"
	newRecord.ExpiresAt = time.Now().Add(2 * time.Hour)
	require.NoError(t, store.Put(ctx, newRecord))
	assert.ErrorIs(t, store.Delete(ctx, old), ErrAsyncJobNotFound)
	got, err := store.Get(ctx, old.Owner, old.PublicID)
	require.NoError(t, err)
	assert.Equal(t, "new", got.BackendJobID)
}

func TestRedisAsyncJobStoreListReturnsCorruptRecordError(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAsyncJobRedisStore(t)
	require.NoError(t, store.client.Set(ctx, asyncJobRedisKey("owner-a", "bad"), "not-json", time.Hour).Err())
	_, err := store.ListByOwner(ctx, "owner-a")
	assert.Error(t, err)
}

func TestAsyncJobRedisPXNeverEndsBeforeExpiry(t *testing.T) {
	assert.Equal(t, int64(1), asyncJobRedisPX(time.Nanosecond))
	assert.Equal(t, int64(2), asyncJobRedisPX(time.Millisecond+time.Nanosecond))
}

func TestRedisAsyncJobStoreDeleteMissingRecord(t *testing.T) {
	store, _ := newTestAsyncJobRedisStore(t)
	err := store.Delete(context.Background(), AsyncJobRecord{Owner: "owner-a", PublicID: "missing"})
	assert.True(t, errors.Is(err, ErrAsyncJobNotFound))
}
