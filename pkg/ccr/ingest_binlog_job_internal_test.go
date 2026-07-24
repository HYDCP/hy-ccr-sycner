// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License

package ccr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	bestruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/backendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	"github.com/tidwall/btree"
)

func newTestTablet(id int64, replicaCount int) *TabletMeta {
	tablet := &TabletMeta{
		Id:           id,
		ReplicaMetas: btree.NewMap[int64, *ReplicaMeta](32),
	}
	for i := 0; i < replicaCount; i++ {
		replica := &ReplicaMeta{
			Id:        int64(i + 1),
			TabletId:  id,
			BackendId: int64(i + 1),
			Version:   10,
		}
		tablet.ReplicaMetas.Set(replica.Id, replica)
	}
	return tablet
}

func newTestHandlerForSingleReplica(attemptsBeforeSuccess int) (*tabletIngestBinlogHandler, *int) {
	callCount := 0
	h := &tabletIngestBinlogHandler{
		ingestJob: &IngestBinlogJob{
			ccrJob: &Job{
				Name:                  "test_job",
				singleReplicaFallback: newFallbackCache(singleReplicaFallbackTTL),
			},
			txnId: 1,
		},
		binlogVersion: 5,
		srcTablet:     newTestTablet(100, 1),
		destTablet:    newTestTablet(200, 3),
		singleReplicaIngestFunc: func(context.Context, []*ReplicaMeta) bool {
			callCount++
			return callCount > attemptsBeforeSuccess
		},
		commitInfosCollector: newCommitInfosCollector(),
		subTxnInfosCollector: newSubTxnInfosCollector(),
	}
	return h, &callCount
}

func TestTrySingleReplicaIngestSuccessOnFirstAttempt(t *testing.T) {
	oldFeature := featureSingleReplicaIngestBinlog
	featureSingleReplicaIngestBinlog = true
	defer func() { featureSingleReplicaIngestBinlog = oldFeature }()

	h, callCount := newTestHandlerForSingleReplica(0)
	if !h.trySingleReplicaIngest(context.Background(), nil) {
		t.Errorf("expected success on first attempt")
	}
	if *callCount != 1 {
		t.Errorf("expected 1 call, got %d", *callCount)
	}
}

func TestTrySingleReplicaIngestSuccessAfterRetries(t *testing.T) {
	oldFeature := featureSingleReplicaIngestBinlog
	featureSingleReplicaIngestBinlog = true
	defer func() { featureSingleReplicaIngestBinlog = oldFeature }()

	oldInterval := flagSingleReplicaIngestRetryInterval
	flagSingleReplicaIngestRetryInterval = 0
	defer func() { flagSingleReplicaIngestRetryInterval = oldInterval }()

	oldMaxRetries := flagSingleReplicaIngestMaxRetries
	flagSingleReplicaIngestMaxRetries = 3
	defer func() { flagSingleReplicaIngestMaxRetries = oldMaxRetries }()

	h, callCount := newTestHandlerForSingleReplica(2)
	if !h.trySingleReplicaIngest(context.Background(), nil) {
		t.Errorf("expected success after retries")
	}
	if *callCount != 3 {
		t.Errorf("expected 3 calls, got %d", *callCount)
	}
}

func TestTrySingleReplicaIngestFallbackAfterMaxRetries(t *testing.T) {
	oldFeature := featureSingleReplicaIngestBinlog
	featureSingleReplicaIngestBinlog = true
	defer func() { featureSingleReplicaIngestBinlog = oldFeature }()

	oldInterval := flagSingleReplicaIngestRetryInterval
	flagSingleReplicaIngestRetryInterval = 0
	defer func() { flagSingleReplicaIngestRetryInterval = oldInterval }()

	oldMaxRetries := flagSingleReplicaIngestMaxRetries
	flagSingleReplicaIngestMaxRetries = 3
	defer func() { flagSingleReplicaIngestMaxRetries = oldMaxRetries }()

	h, callCount := newTestHandlerForSingleReplica(100)
	if h.trySingleReplicaIngest(context.Background(), nil) {
		t.Errorf("expected fallback after max retries")
	}
	if *callCount != 3 {
		t.Errorf("expected 3 calls, got %d", *callCount)
	}
}

func TestTrySingleReplicaIngestCanceled(t *testing.T) {
	oldFeature := featureSingleReplicaIngestBinlog
	featureSingleReplicaIngestBinlog = true
	defer func() { featureSingleReplicaIngestBinlog = oldFeature }()

	oldInterval := flagSingleReplicaIngestRetryInterval
	flagSingleReplicaIngestRetryInterval = time.Hour
	defer func() { flagSingleReplicaIngestRetryInterval = oldInterval }()

	h, callCount := newTestHandlerForSingleReplica(100)
	h.cancel.Store(true)
	if h.trySingleReplicaIngest(context.Background(), nil) {
		t.Errorf("expected false when canceled")
	}
	if *callCount != 0 {
		t.Errorf("expected 0 calls when canceled, got %d", *callCount)
	}
}

func TestTrySingleReplicaIngestDisabled(t *testing.T) {
	oldFeature := featureSingleReplicaIngestBinlog
	featureSingleReplicaIngestBinlog = false
	defer func() { featureSingleReplicaIngestBinlog = oldFeature }()

	h, callCount := newTestHandlerForSingleReplica(0)
	if h.trySingleReplicaIngest(context.Background(), nil) {
		t.Errorf("expected false when feature disabled")
	}
	if *callCount != 0 {
		t.Errorf("expected 0 calls when feature disabled, got %d", *callCount)
	}
}

func TestTrySingleReplicaIngestSingleReplicaTablet(t *testing.T) {
	oldFeature := featureSingleReplicaIngestBinlog
	featureSingleReplicaIngestBinlog = true
	defer func() { featureSingleReplicaIngestBinlog = oldFeature }()

	h, callCount := newTestHandlerForSingleReplica(0)
	h.destTablet = newTestTablet(200, 1)
	if h.trySingleReplicaIngest(context.Background(), nil) {
		t.Errorf("expected false for single replica tablet")
	}
	if *callCount != 0 {
		t.Errorf("expected 0 calls for single replica tablet, got %d", *callCount)
	}
}

// --- mocks for handleSingleReplica tests ---

type mockBeRpc struct {
	result *bestruct.TIngestBinlogResult_
	err    error
}

func (m *mockBeRpc) IngestBinlog(ctx context.Context, req *bestruct.TIngestBinlogRequest, beOptions ...rpc.BeRpcOption) (*bestruct.TIngestBinlogResult_, error) {
	return m.result, m.err
}

type mockRpcFactory struct {
	beRpc rpc.IBeRpc
	err   error
}

func (f *mockRpcFactory) NewFeRpc(spec *base.Spec) (rpc.IFeRpc, error) {
	return nil, nil
}

func (f *mockRpcFactory) NewBeRpc(be *base.Backend) (rpc.IBeRpc, error) {
	return f.beRpc, f.err
}

func newTestIngestBinlogJob(beRpc rpc.IBeRpc, newBeErr error) *IngestBinlogJob {
	return &IngestBinlogJob{
		ccrJob: &Job{
			Name:                  "test_job",
			concurrencyManager:    rpc.NewConcurrencyManager(),
			singleReplicaFallback: newFallbackCache(singleReplicaFallbackTTL),
		},
		factory: &Factory{IRpcFactory: &mockRpcFactory{beRpc: beRpc, err: newBeErr}},
		txnId:   1,
		srcBackendMap: map[int64]*base.Backend{
			1: {Id: 1, Host: "src", BePort: 9050, HttpPort: 8040},
		},
		destBackendMap: map[int64]*base.Backend{
			1: {Id: 1, Host: "be1", BePort: 9050, HttpPort: 8040},
			2: {Id: 2, Host: "be2", BePort: 9050, HttpPort: 8040},
			3: {Id: 3, Host: "be3", BePort: 9050, HttpPort: 8040},
		},
		commitInfosCollector: newCommitInfosCollector(),
		subTxnInfosCollector: newSubTxnInfosCollector(),
	}
}

func newTestHandlerForHandleSingleReplica(beRpc rpc.IBeRpc, newBeErr error) (*tabletIngestBinlogHandler, []*ReplicaMeta) {
	srcTablet := newTestTablet(100, 1)
	destTablet := newTestTablet(200, 3)

	var srcReplicas []*ReplicaMeta
	srcTablet.ReplicaMetas.Scan(func(_ int64, r *ReplicaMeta) bool {
		srcReplicas = append(srcReplicas, r)
		return true
	})

	h := &tabletIngestBinlogHandler{
		ingestJob:            newTestIngestBinlogJob(beRpc, newBeErr),
		binlogVersion:        5,
		destPartitionId:      1000,
		srcTablet:            srcTablet,
		destTablet:           destTablet,
		commitInfosCollector: newCommitInfosCollector(),
		subTxnInfosCollector: newSubTxnInfosCollector(),
	}
	return h, srcReplicas
}

func TestHandleSingleReplicaSuccess(t *testing.T) {
	// destTablet id=200, 3 replicas -> leaderIdx = 200 % 3 = 2 (backend 3), followers backend 1 & 2
	result := &bestruct.TIngestBinlogResult_{
		Status:                   &tstatus.TStatus{StatusCode: tstatus.TStatusCode_OK},
		SuccessReplicaBackendIds: []int64{1, 2},
	}

	h, srcReplicas := newTestHandlerForHandleSingleReplica(&mockBeRpc{result: result}, nil)
	if !h.handleSingleReplica(context.Background(), srcReplicas) {
		t.Errorf("expected success")
	}
	commitInfos := h.CommitInfos()
	if len(commitInfos) != 3 {
		t.Fatalf("expected 3 commit infos (leader + 2 followers), got %d", len(commitInfos))
	}
	backendIds := make(map[int64]bool)
	for _, ci := range commitInfos {
		backendIds[ci.BackendId] = true
	}
	for _, id := range []int64{1, 2, 3} {
		if !backendIds[id] {
			t.Errorf("missing commit info for backend %d", id)
		}
	}
}

func TestHandleSingleReplicaPartialFailure(t *testing.T) {
	// follower backend 2 failed, only backend 1 succeeded
	result := &bestruct.TIngestBinlogResult_{
		Status:                   &tstatus.TStatus{StatusCode: tstatus.TStatusCode_OK},
		SuccessReplicaBackendIds: []int64{1},
		FailedReplicaBackendIds:  []int64{2},
	}

	h, srcReplicas := newTestHandlerForHandleSingleReplica(&mockBeRpc{result: result}, nil)
	if h.handleSingleReplica(context.Background(), srcReplicas) {
		t.Errorf("expected failure due to partial follower failure")
	}
	if len(h.CommitInfos()) != 0 {
		t.Errorf("expected no commit infos on partial failure, got %d", len(h.CommitInfos()))
	}
}

func TestHandleSingleReplicaOldBeFallback(t *testing.T) {
	// Old BE does not set success_replica_backend_ids
	result := &bestruct.TIngestBinlogResult_{
		Status: &tstatus.TStatus{StatusCode: tstatus.TStatusCode_OK},
	}

	h, srcReplicas := newTestHandlerForHandleSingleReplica(&mockBeRpc{result: result}, nil)
	if h.handleSingleReplica(context.Background(), srcReplicas) {
		t.Errorf("expected fallback for old BE")
	}
	if len(h.CommitInfos()) != 0 {
		t.Errorf("expected no commit infos on old BE fallback, got %d", len(h.CommitInfos()))
	}
}

func TestHandleSingleReplicaRpcError(t *testing.T) {
	h, srcReplicas := newTestHandlerForHandleSingleReplica(&mockBeRpc{err: errors.New("rpc error")}, nil)
	if h.handleSingleReplica(context.Background(), srcReplicas) {
		t.Errorf("expected failure on rpc error")
	}
	if len(h.CommitInfos()) != 0 {
		t.Errorf("expected no commit infos on rpc error, got %d", len(h.CommitInfos()))
	}
}

func TestHandleSingleReplicaNewBeRpcError(t *testing.T) {
	h, srcReplicas := newTestHandlerForHandleSingleReplica(nil, errors.New("new be rpc error"))
	if h.handleSingleReplica(context.Background(), srcReplicas) {
		t.Errorf("expected failure when NewBeRpc fails")
	}
	if len(h.CommitInfos()) != 0 {
		t.Errorf("expected no commit infos when NewBeRpc fails, got %d", len(h.CommitInfos()))
	}
}

func TestHandleSingleReplicaSingleReplicaTablet(t *testing.T) {
	h, srcReplicas := newTestHandlerForHandleSingleReplica(&mockBeRpc{result: &bestruct.TIngestBinlogResult_{}}, nil)
	h.destTablet = newTestTablet(200, 1)
	if h.handleSingleReplica(context.Background(), srcReplicas) {
		t.Errorf("expected failure for single replica tablet")
	}
}

func TestFallbackCacheTTL(t *testing.T) {
	cache := newFallbackCache(100 * time.Millisecond)

	if cache.Load() {
		t.Fatal("new cache should report false")
	}

	cache.Store()
	if !cache.Load() {
		t.Fatal("cache should report true immediately after Store")
	}

	time.Sleep(150 * time.Millisecond)
	if cache.Load() {
		t.Fatal("cache should expire after TTL")
	}

	// After expiration, Store() should work again.
	cache.Store()
	if !cache.Load() {
		t.Fatal("cache should report true after re-Store")
	}

	cache.Clear()
	if cache.Load() {
		t.Fatal("cache should report false after Clear")
	}
}
