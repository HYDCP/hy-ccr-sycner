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
					collector := NewJobCollector(db, "syncer", &ccr.Factory{IRpcFactory: rpcFactory})
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
