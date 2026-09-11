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
	"errors"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testAsyncJobRegistration(owner, model string) AsyncJobRegistration {
	return AsyncJobRegistration{
		JobType:      "video",
		Owner:        owner,
		Model:        model,
		BackendJobID: "backend-video-1",
		BackendPod: AsyncJobBackendPod{
			Namespace: "default",
			Name:      "video-0",
			UID:       "pod-uid-1",
		},
		ExpiresAt: time.Now().Add(time.Hour),
	}
}

func TestAsyncJobRegistry_RegisterGetDeleteAndOwnership(t *testing.T) {
	ctx := context.Background()
	registry := NewAsyncJobRegistry(NewInMemoryAsyncJobStore())

	job, err := registry.Register(ctx, testAsyncJobRegistration("alice", "wan2.1"))
	require.NoError(t, err)
	assert.NotEmpty(t, job.PublicJobID)
	assert.Equal(t, "backend-video-1", job.BackendJobID)
	assert.Equal(t, "pod-uid-1", job.BackendPod.UID)
	assert.False(t, job.CreatedAt.IsZero())

	got, err := registry.Get(ctx, job.PublicJobID, "alice")
	require.NoError(t, err)
	assert.Equal(t, job, got)

	_, err = registry.Get(ctx, job.PublicJobID, "bob")
	assert.ErrorIs(t, err, ErrAsyncJobNotFound, "ownership mismatches must not disclose that a job exists")

	err = registry.Delete(ctx, job.PublicJobID, "alice")
	require.NoError(t, err)
	_, err = registry.Get(ctx, job.PublicJobID, "alice")
	assert.ErrorIs(t, err, ErrAsyncJobNotFound)
}

func TestAsyncJobRegistry_ListFiltersOwnerTypeModelAndExpiredJobs(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryAsyncJobStore()
	registry := NewAsyncJobRegistry(store)

	aliceVideo, err := registry.Register(ctx, testAsyncJobRegistration("alice", "wan2.1"))
	require.NoError(t, err)
	_, err = registry.Register(ctx, testAsyncJobRegistration("alice", "other-model"))
	require.NoError(t, err)
	_, err = registry.Register(ctx, testAsyncJobRegistration("bob", "wan2.1"))
	require.NoError(t, err)

	expired := aliceVideo
	expired.PublicJobID = "expired-job"
	expired.ExpiresAt = time.Now().Add(-time.Second)
	require.NoError(t, store.Create(ctx, expired))

	jobs, err := registry.List(ctx, "alice", AsyncJobListOptions{JobType: "video", Model: "wan2.1"})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, aliceVideo.PublicJobID, jobs[0].PublicJobID)

	_, err = store.Get(ctx, expired.PublicJobID)
	assert.ErrorIs(t, err, ErrAsyncJobNotFound, "list must clean up expired records")
}

func TestRedisAsyncJobStoreSharesJobsAcrossRegistries(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	store := NewRedisAsyncJobStore(client)
	writer := NewAsyncJobRegistry(store)
	reader := NewAsyncJobRegistry(store)

	job, err := writer.Register(ctx, testAsyncJobRegistration("alice", "wan2.1"))
	require.NoError(t, err)

	got, err := reader.Get(ctx, job.PublicJobID, "alice")
	require.NoError(t, err)
	assert.Equal(t, job, got)

	jobs, err := reader.List(ctx, "alice", AsyncJobListOptions{JobType: "video", Model: "wan2.1"})
	require.NoError(t, err)
	assert.Equal(t, []AsyncJobRecord{job}, jobs)

	err = reader.Delete(ctx, job.PublicJobID, "alice")
	require.NoError(t, err)
	_, err = writer.Get(ctx, job.PublicJobID, "alice")
	assert.True(t, errors.Is(err, ErrAsyncJobNotFound))
}
