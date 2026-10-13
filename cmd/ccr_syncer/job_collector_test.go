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
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	"github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/xmetrics"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// The collector only needs these three reads. Leave unrelated DB methods unused.
type jobCollectorTestDB struct {
	storage.DB
	jobName  string
	jobInfo  string
	progress string
}

func (db *jobCollectorTestDB) GetStampAndJobs(string) (int64, []string, error) {
	return 0, []string{db.jobName}, nil
}

func (db *jobCollectorTestDB) GetJobInfo(string) (string, error) {
	return db.jobInfo, nil
}

func (db *jobCollectorTestDB) GetProgress(string) (string, error) {
	return db.progress, nil
}

// Adapt only the lag method: the generated FE mock predates other RPC signatures.
type jobCollectorTestFeRPC struct {
	rpc.IFeRpc
	mock *ccr.MockIFeRpc
}

func (fe *jobCollectorTestFeRPC) GetBinlogLag(spec *base.Spec, commitSeq int64) (*frontendservice.TGetBinlogLagResult_, error) {
	return fe.mock.GetBinlogLag(spec, commitSeq)
}

// Metric vectors are global, so use distinct labels even with go test -count.
var jobCollectorTestSequence uint64

func TestJobCollectorUpdateMetrics(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     tstatus.TStatusCode
		rpcErr     error
		factoryErr error
	}{
		{name: "ok", status: tstatus.TStatusCode_OK},
		{name: "table_not_found", status: tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE},
		{name: "binlog_too_old", status: tstatus.TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ},
		{name: "rpc_error", rpcErr: errors.New("lag RPC failed")},
		{name: "factory_error", factoryErr: errors.New("FE RPC creation failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, existingMetrics := range []bool{false, true} {
				t.Run(fmt.Sprintf("existing_metrics=%t", existingMetrics), func(t *testing.T) {
					ctrl := gomock.NewController(t)
					rpcFactory := ccr.NewMockIRpcFactory(ctrl)
					feRPC := ccr.NewMockIFeRpc(ctrl)
					lagRPC := &jobCollectorTestFeRPC{mock: feRPC}
					jobName := fmt.Sprintf("%s/%d", t.Name(), atomic.AddUint64(&jobCollectorTestSequence, 1))
					src := base.Spec{Frontend: base.Frontend{Host: "source-fe"}, Table: "source_table"}
					jobJSON, err := json.Marshal(&ccr.Job{Name: jobName, State: ccr.JobRunning, Src: src})
					require.NoError(t, err)
					db := &jobCollectorTestDB{jobName: jobName, jobInfo: string(jobJSON)}
					collector := NewJobCollector(db, "syncer", &ccr.Factory{IRpcFactory: rpcFactory}, nil)
					progress := &ccr.JobProgress{CommitSeq: 100}
					setProgress := func(state ccr.SyncState, subState ccr.SubSyncState) {
						progress.SyncState = state
						progress.SubSyncState = subState
						data, err := json.Marshal(progress)
						require.NoError(t, err)
						db.progress = string(data)
					}

					if existingMetrics {
						setProgress(ccr.TableIncrementalSync, ccr.Done)
						rpcFactory.EXPECT().NewFeRpc(gomock.Eq(&src)).Return(lagRPC, nil)
						feRPC.EXPECT().GetBinlogLag(gomock.Eq(&src), progress.CommitSeq).
							Return(jobCollectorLagResponse(23, 7000), nil)
						require.NoError(t, collector.updateMetrics())
						require.Equal(t, map[string]float64{
							"ccr_job_running_sync_state":     float64(ccr.TableIncrementalSync),
							"ccr_job_running_sub_sync_state": float64(ccr.Done.State),
							"ccr_job_running_lag_total":      23,
							"ccr_job_running_lag_seconds":    7,
						}, jobCollectorMetrics(t, jobName))
					}

					setProgress(ccr.TableFullSync, ccr.BeginCreateSnapshot)
					if tc.factoryErr != nil {
						rpcFactory.EXPECT().NewFeRpc(gomock.Eq(&src)).Return(nil, tc.factoryErr)
					} else {
						rpcFactory.EXPECT().NewFeRpc(gomock.Eq(&src)).Return(lagRPC, nil)
						var response *frontendservice.TGetBinlogLagResult_
						if tc.rpcErr == nil {
							if tc.status == tstatus.TStatusCode_OK {
								response = jobCollectorLagResponse(11, 3000)
							} else {
								// FE error responses have no usable lag fields.
								response = &frontendservice.TGetBinlogLagResult_{
									Status: &tstatus.TStatus{StatusCode: tc.status},
								}
							}
						}
						feRPC.EXPECT().GetBinlogLag(gomock.Eq(&src), progress.CommitSeq).
							Return(response, tc.rpcErr)
					}
					require.NoError(t, collector.updateMetrics())

					want := map[string]float64{
						"ccr_job_running_sync_state":     float64(ccr.TableFullSync),
						"ccr_job_running_sub_sync_state": float64(ccr.BeginCreateSnapshot.State),
					}
					if tc.status == tstatus.TStatusCode_OK && tc.rpcErr == nil && tc.factoryErr == nil {
						want["ccr_job_running_lag_total"] = 11
						want["ccr_job_running_lag_seconds"] = 3
					} else if existingMetrics {
						want["ccr_job_running_lag_total"] = 23
						want["ccr_job_running_lag_seconds"] = 7
					}
					// On an initial failure, state metrics exist but lag metrics must not be fabricated.
					require.Equal(t, want, jobCollectorMetrics(t, jobName))
				})
			}
		})
	}
}

func jobCollectorLagResponse(lag, intervalMs int64) *frontendservice.TGetBinlogLagResult_ {
	nextTimestamp := int64(1000)
	lastTimestamp := nextTimestamp + intervalMs
	return &frontendservice.TGetBinlogLagResult_{
		Status:              &tstatus.TStatus{StatusCode: tstatus.TStatusCode_OK},
		Lag:                 &lag,
		NextBinlogTimestamp: &nextTimestamp,
		LastBinlogTimestamp: &lastTimestamp,
	}
}

func jobCollectorMetrics(t *testing.T, jobName string) map[string]float64 {
	t.Helper()
	response := httptest.NewRecorder()
	xmetrics.GetHttpHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, response.Code)
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(response.Body)
	require.NoError(t, err)
	values := make(map[string]float64)
	for _, name := range []string{
		"ccr_job_running_sync_state", "ccr_job_running_sub_sync_state",
		"ccr_job_running_lag_total", "ccr_job_running_lag_seconds",
	} {
		for _, metric := range families[name].GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "name" && label.GetValue() == jobName {
					values[name] = metric.GetGauge().GetValue()
				}
			}
		}
	}
	return values
}

// lagTestDB serves several jobs; a job without info fails to load.
type lagTestDB struct {
	storage.DB
	jobs     []string
	listErr  error
	infos    map[string]string
	progress string
}

func (db *lagTestDB) GetStampAndJobs(string) (int64, []string, error) {
	return 0, db.jobs, db.listErr
}

func (db *lagTestDB) GetJobInfo(name string) (string, error) {
	if info, ok := db.infos[name]; ok {
		return info, nil
	}
	return "", errors.New("job info not found")
}

func (db *lagTestDB) GetProgress(string) (string, error) {
	return db.progress, nil
}

// lagTestFeRPC answers GetBinlogLag per source table.
type lagTestFeRPC struct {
	rpc.IFeRpc
	lags map[string]*frontendservice.TGetBinlogLagResult_
}

func (fe *lagTestFeRPC) GetBinlogLag(spec *base.Spec, _ int64) (*frontendservice.TGetBinlogLagResult_, error) {
	if resp, ok := fe.lags[spec.Table]; ok {
		return resp, nil
	}
	return nil, errors.New("lag RPC failed")
}

type lagTestRPCFactory struct{ fe rpc.IFeRpc }

func (f *lagTestRPCFactory) NewFeRpc(*base.Spec) (rpc.IFeRpc, error) { return f.fe, nil }
func (f *lagTestRPCFactory) NewBeRpc(*base.Backend) (rpc.IBeRpc, error) {
	return nil, errors.New("unused")
}

func TestJobCollectorPublishesWholeLagSnapshots(t *testing.T) {
	prefix := fmt.Sprintf("lag_snapshot_%d_", atomic.AddUint64(&jobCollectorTestSequence, 1))
	jobInfo := func(name string) string {
		data, err := json.Marshal(&ccr.Job{Name: prefix + name, State: ccr.JobRunning, Src: base.Spec{Table: name}})
		require.NoError(t, err)
		return string(data)
	}
	progress, err := json.Marshal(&ccr.JobProgress{SyncState: ccr.TableIncrementalSync, SubSyncState: ccr.Done, CommitSeq: 100})
	require.NoError(t, err)
	db := &lagTestDB{
		jobs:     []string{prefix + "ok", prefix + "flaky", prefix + "gone"},
		infos:    map[string]string{prefix + "ok": jobInfo("ok"), prefix + "flaky": jobInfo("flaky"), prefix + "gone": jobInfo("gone")},
		progress: string(progress),
	}
	fe := &lagTestFeRPC{lags: map[string]*frontendservice.TGetBinlogLagResult_{
		"ok":    jobCollectorLagResponse(5, 2000),
		"flaky": jobCollectorLagResponse(6, 3000),
		"gone":  jobCollectorLagResponse(7, 4000),
	}}
	var published []*ccr.JobLagSnapshot
	collector := NewJobCollector(db, "syncer", &ccr.Factory{IRpcFactory: &lagTestRPCFactory{fe: fe}},
		func(snapshot *ccr.JobLagSnapshot) { published = append(published, snapshot) })

	before := time.Now().UnixMilli()
	require.NoError(t, collector.updateMetrics())
	require.Len(t, published, 1)
	first := published[0]
	require.GreaterOrEqual(t, first.GeneratedAt, before)
	require.Len(t, first.Jobs, 3)
	okLag := first.Jobs[prefix+"ok"]
	require.Equal(t, int64(5), okLag.Lag)
	require.Equal(t, int64(1000), okLag.NextBinlogTimestampMs)
	require.Equal(t, int64(100), okLag.CommitSeq)
	require.GreaterOrEqual(t, okLag.ComputedAt, before)
	require.Empty(t, okLag.Error)
	firstFlaky := first.Jobs[prefix+"flaky"]

	// Round 2: the flaky lag RPC fails, gone is no longer on this syncer, and
	// the info of a new job cannot be loaded.
	fe.lags["ok"] = jobCollectorLagResponse(0, 0)
	fe.lags["ok"].NextBinlogTimestamp = func() *int64 { v := int64(-1); return &v }()
	delete(fe.lags, "flaky")
	db.jobs = []string{prefix + "ok", prefix + "flaky", prefix + "new"}
	require.NoError(t, collector.updateMetrics())
	require.Len(t, published, 2)
	second := published[1]
	require.Len(t, second.Jobs, 3)
	require.Equal(t, int64(0), second.Jobs[prefix+"ok"].Lag)
	require.Equal(t, int64(-1), second.Jobs[prefix+"ok"].NextBinlogTimestampMs)
	flaky := second.Jobs[prefix+"flaky"]
	require.Equal(t, firstFlaky.Lag, flaky.Lag)
	require.Equal(t, firstFlaky.ComputedAt, flaky.ComputedAt)
	require.Contains(t, flaky.Error, "lag RPC failed")
	newJob := second.Jobs[prefix+"new"]
	require.Zero(t, newJob.ComputedAt)
	require.Contains(t, newJob.Error, "job info not found")
	require.NotContains(t, second.Jobs, prefix+"gone")

	// The published snapshot is replaced, never modified.
	require.Len(t, first.Jobs, 3)
	require.Equal(t, okLag, first.Jobs[prefix+"ok"])
	require.Empty(t, first.Jobs[prefix+"flaky"].Error)

	// A round that cannot list the jobs keeps the previous snapshot.
	db.listErr = errors.New("meta db down")
	require.Error(t, collector.updateMetrics())
	require.Len(t, published, 2)
}
