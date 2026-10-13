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
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// gapTestFeRpc answers GetBinlog with a fixed response; other FE calls are unused.
type gapTestFeRpc struct {
	rpc.IFeRpc
	resp          *festruct.TGetBinlogResult_
	prevCommitSeq int64
}

func (fe *gapTestFeRpc) GetBinlog(_ *base.Spec, prevCommitSeq, _ int64) (*festruct.TGetBinlogResult_, error) {
	fe.prevCommitSeq = prevCommitSeq
	return fe.resp, nil
}

type gapTestRpcFactory struct{ fe rpc.IFeRpc }

func (f *gapTestRpcFactory) NewFeRpc(*base.Spec) (rpc.IFeRpc, error) { return f.fe, nil }
func (f *gapTestRpcFactory) NewBeRpc(*base.Backend) (rpc.IBeRpc, error) {
	return nil, errors.New("unused")
}

func newGapTestJob(resp *festruct.TGetBinlogResult_) (*Job, *gapTestFeRpc) {
	fe := &gapTestFeRpc{resp: resp}
	return &Job{
		Name:     "gap_job",
		SyncType: TableSync,
		factory:  &Factory{IRpcFactory: &gapTestRpcFactory{fe: fe}},
		progress: &JobProgress{
			SyncState:     TableIncrementalSync,
			SubSyncState:  Done,
			PrevCommitSeq: 100,
			CommitSeq:     100,
			PrevTxnId:     -1,
		},
		pipelineCtx: &JobPipelineContext{NextCommitSeq: 100},
	}, fe
}

func tooOldResponse(binlogs ...*festruct.TBinlog) *festruct.TGetBinlogResult_ {
	return &festruct.TGetBinlogResult_{
		Status:  &tstatus.TStatus{StatusCode: tstatus.TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ},
		Binlogs: binlogs,
	}
}

func TestBinlogTooOldIsReportedAtEverySite(t *testing.T) {
	commitSeq, dummy := int64(150), festruct.TBinlogType_DUMMY
	for _, tc := range []struct {
		name   string
		source string
		call   func(j *Job) error
	}{
		{name: "incremental", source: "incremental sync", call: func(j *Job) error { return j.incrementalSyncInternal() }},
		{name: "pipeline", source: "pipeline sync", call: func(j *Job) error { return j.getNextBinlogs() }},
		{name: "specified", source: "get specified binlog", call: func(j *Job) error {
			_, err := j.GetSpecifiedBinlog(101)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()

			// The incremental and pipeline sites get no binlogs, so they stop at
			// the existing "no binlog" check right after the status switch; the
			// specified site gets a binlog with another commit seq and fails the
			// existing commit seq check. Either way the old flow is kept.
			resp := tooOldResponse()
			if tc.name == "specified" {
				resp = tooOldResponse(&festruct.TBinlog{CommitSeq: &commitSeq, Type: &dummy})
			}
			job, fe := newGapTestJob(resp)

			before := time.Now().UnixMilli()
			require.Error(t, tc.call(job))
			require.Equal(t, int64(100), fe.prevCommitSeq)

			status := job.Status()
			require.True(t, status.BinlogGapSuspected)
			require.Equal(t, int64(100), status.BinlogGapCommitSeq)
			require.GreaterOrEqual(t, status.BinlogGapFirstSeenAt, before)

			var reported *logrus.Entry
			for _, entry := range hook.AllEntries() {
				if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, "binlog too old") {
					reported = entry
				}
			}
			require.NotNil(t, reported, "missing ERROR log")
			require.Contains(t, reported.Message, "job: gap_job")
			require.Contains(t, reported.Message, "commit seq 100")
			require.Contains(t, reported.Message, "source: "+tc.source)
		})
	}
}

func TestBinlogGapMarkerKeepsFirstSeenAndClearsAfterFullSync(t *testing.T) {
	job, _ := newGapTestJob(nil)
	first := time.UnixMilli(1_000)
	job.binlogGap.mark(100, first)
	job.binlogGap.mark(180, first.Add(time.Minute)) // only logged

	require.Equal(t, BinlogGapStatus{
		BinlogGapSuspected:   true,
		BinlogGapFirstSeenAt: 1_000,
		BinlogGapCommitSeq:   100,
	}, job.Status().BinlogGapStatus)

	// A full sync that is still running keeps the marker.
	for _, state := range []SyncState{TableFullSync, DBFullSync} {
		job.progress.SyncState = state
		job.clearBinlogGapIfFullSynced()
		require.True(t, job.Status().BinlogGapSuspected, state.String())
	}

	// Once the full sync has switched to incremental sync, the marker is cleared.
	job.progress.SyncState = TableIncrementalSync
	job.clearBinlogGapIfFullSynced()
	require.Equal(t, BinlogGapStatus{}, job.Status().BinlogGapStatus)
}

func TestJobStatusBinlogGapJSON(t *testing.T) {
	gapFields := func(status *JobStatus) map[string]any {
		data, err := json.Marshal(status)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(data, &fields))
		for key := range fields {
			if !strings.HasPrefix(key, "binlog_gap") {
				delete(fields, key)
			}
		}
		return fields
	}

	job, _ := newGapTestJob(nil)
	// Optional fields: only the flag is always present.
	require.Equal(t, map[string]any{"binlog_gap_suspected": false}, gapFields(job.Status()))

	job.binlogGap.mark(100, time.UnixMilli(1_000))
	require.Equal(t, map[string]any{
		"binlog_gap_suspected":     true,
		"binlog_gap_first_seen_at": float64(1000),
		"binlog_gap_commit_seq":    float64(100),
	}, gapFields(job.Status()))

	// The marker is never persisted with the job info.
	data, err := json.Marshal(job)
	require.NoError(t, err)
	require.NotContains(t, string(data), "binlog_gap")
}
