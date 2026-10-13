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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
	"github.com/stretchr/testify/require"
)

func TestJobErrorStatsStreak(t *testing.T) {
	var stats jobErrorStats
	at := func(ms int64) time.Time { return time.UnixMilli(ms) }

	stats.recordFailure(xerror.Errorf(xerror.RPC, "rpc down"), false, at(1_000))
	stats.recordFailure(xerror.Wrap(errors.New("conn reset"), xerror.FE, "get binlog"), false, at(2_000))
	status := stats.get()
	require.Equal(t, int64(2), status.ConsecutiveFailures)
	require.Equal(t, int64(1_000), status.FirstFailureAt)
	require.Equal(t, int64(2_000), status.LastErrorAt)
	require.Equal(t, "fe", status.LastErrorCategory)
	require.Equal(t, "get binlog: [fe] conn reset", status.LastError)
	require.Zero(t, status.LastSuccessAt)
	require.Equal(t, []JobErrorRecord{
		{At: 2_000, Category: "fe", Message: "get binlog: [fe] conn reset", Count: 1},
		{At: 1_000, Category: "rpc", Message: "[rpc] rpc down", Count: 1},
	}, status.RecentErrors)

	// A successful round ends the streak but keeps the last error for diagnosis.
	stats.recordSuccess(at(3_000))
	status = stats.get()
	require.Zero(t, status.ConsecutiveFailures)
	require.Zero(t, status.FirstFailureAt)
	require.Equal(t, int64(3_000), status.LastSuccessAt)
	require.Equal(t, "fe", status.LastErrorCategory)
	require.Len(t, status.RecentErrors, 2)

	// A new streak starts at the next failure; non-xerror errors are "unknown".
	stats.recordFailure(errors.New("plain"), false, at(4_000))
	status = stats.get()
	require.Equal(t, int64(1), status.ConsecutiveFailures)
	require.Equal(t, int64(4_000), status.FirstFailureAt)
	require.Equal(t, "unknown", status.LastErrorCategory)
}

func TestJobErrorStatsCommitSeqAdvanceCountsAsSuccess(t *testing.T) {
	var stats jobErrorStats
	// The first observation only sets the baseline, e.g. right after a restart.
	stats.observeCommitSeq(100, time.UnixMilli(500))
	stats.recordFailure(xerror.Errorf(xerror.BE, "ingest failed"), false, time.UnixMilli(1_000))
	stats.observeCommitSeq(100, time.UnixMilli(1_500))
	stats.observeCommitSeq(90, time.UnixMilli(1_600))
	status := stats.get()
	require.Equal(t, int64(1), status.ConsecutiveFailures)
	require.Zero(t, status.LastSuccessAt)

	// Synced binlogs in the middle of a long round end the streak at once.
	stats.observeCommitSeq(101, time.UnixMilli(2_000))
	status = stats.get()
	require.Zero(t, status.ConsecutiveFailures)
	require.Zero(t, status.FirstFailureAt)
	require.Equal(t, int64(2_000), status.LastSuccessAt)
}

func TestUpdateJobStatusObservesSyncedCommitSeq(t *testing.T) {
	job := &Job{Name: "stats_job", progress: &JobProgress{PrevCommitSeq: 100}}
	job.updateJobStatus()
	job.errStats.recordFailure(xerror.Errorf(xerror.BE, "ingest failed"), false, time.Now())
	job.updateJobStatus()
	require.Equal(t, int64(1), job.Status().ConsecutiveFailures)

	// Both the classic and the pipeline path call updateJobStatus after a
	// binlog is committed.
	job.progress.PrevCommitSeq = 101
	job.updateJobStatus()
	status := job.Status()
	require.Zero(t, status.ConsecutiveFailures)
	require.NotZero(t, status.LastSuccessAt)
}

func TestJobErrorStatsMergesRepeatedErrors(t *testing.T) {
	var stats jobErrorStats
	timeout := func() error { return xerror.Errorf(xerror.RPC, "connect fe timeout") }
	for ms := int64(1_000); ms <= 3_000; ms += 1_000 {
		stats.recordFailure(timeout(), false, time.UnixMilli(ms))
	}
	// Same message, other category: a new entry.
	stats.recordFailure(xerror.Errorf(xerror.FE, "connect fe timeout"), false, time.UnixMilli(4_000))
	stats.recordFailure(timeout(), false, time.UnixMilli(5_000))
	// A success in between ends the streak, so the same error starts a new entry.
	stats.recordSuccess(time.UnixMilli(6_000))
	stats.recordFailure(timeout(), false, time.UnixMilli(7_000))
	stats.recordFailure(timeout(), false, time.UnixMilli(8_000))

	status := stats.get()
	require.Equal(t, int64(2), status.ConsecutiveFailures)
	require.Equal(t, int64(7_000), status.FirstFailureAt)
	require.Equal(t, int64(8_000), status.LastErrorAt)
	require.Equal(t, []JobErrorRecord{
		{At: 8_000, Category: "rpc", Message: "[rpc] connect fe timeout", Count: 2},
		{At: 5_000, Category: "rpc", Message: "[rpc] connect fe timeout", Count: 1},
		{At: 4_000, Category: "fe", Message: "[fe] connect fe timeout", Count: 1},
		{At: 3_000, Category: "rpc", Message: "[rpc] connect fe timeout", Count: 3},
	}, status.RecentErrors)
}

func TestJobErrorStatsKeepsTenNewestTruncatedErrors(t *testing.T) {
	var stats jobErrorStats
	for i := 1; i <= 12; i++ {
		stats.recordFailure(fmt.Errorf("error %d", i), false, time.UnixMilli(int64(i)))
	}
	status := stats.get()
	require.Equal(t, int64(12), status.ConsecutiveFailures)
	require.Len(t, status.RecentErrors, 10)
	for i, record := range status.RecentErrors {
		require.Equal(t, fmt.Sprintf("error %d", 12-i), record.Message)
	}

	// 1KB at most, never splitting a multi-byte character.
	long := "x" + strings.Repeat("数据", 600)
	stats.recordFailure(errors.New(long), false, time.UnixMilli(13))
	status = stats.get()
	require.LessOrEqual(t, len(status.LastError), 1024)
	require.Greater(t, len(status.LastError), 1020)
	require.True(t, utf8.ValidString(status.LastError))
	require.True(t, strings.HasPrefix(long, status.LastError))
	require.Equal(t, status.LastError, status.RecentErrors[0].Message)
	require.Equal(t, "error 12", status.RecentErrors[1].Message)
}

func TestJobErrorStatsConcurrentReaders(t *testing.T) {
	job := &Job{Name: "stats_job"}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(2)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				job.errStats.recordFailure(fmt.Errorf("writer %d error %d", w, i), i%50 == 0, time.Now())
				job.errStats.observeCommitSeq(int64(i), time.Now())
				job.errStats.recordSuccess(time.Now())
			}
		}(w)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				status := job.Status()
				// Callers own their copy.
				for k := range status.RecentErrors {
					status.RecentErrors[k].Message = ""
				}
				_, _ = json.Marshal(status)
			}
		}()
	}
	wg.Wait()

	status := job.Status()
	require.True(t, status.Panicked)
	require.Len(t, status.RecentErrors, 10)
	for _, record := range status.RecentErrors {
		require.NotEmpty(t, record.Message)
	}
}

func TestJobStatusErrorJSON(t *testing.T) {
	job := &Job{Name: "stats_job"}
	data, err := json.Marshal(job.Status())
	require.NoError(t, err)
	// Optional fields: only the counters and flags are always present.
	require.JSONEq(t, `{"name":"stats_job","state":"running","progress_state":"DBFullSync","commit_seq":0,
		"binlog_gap_suspected":false,"consecutive_failures":0,"panicked":false}`, string(data))

	job.errStats.recordFailure(xerror.Errorf(xerror.Meta, "table not found"), false, time.UnixMilli(1_000))
	job.errStats.recordSuccess(time.UnixMilli(2_000))
	job.errStats.recordFailure(xerror.Panicf(xerror.Normal, "bad state"), true, time.UnixMilli(3_000))
	data, err = json.Marshal(job.Status())
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"stats_job","state":"running","progress_state":"DBFullSync","commit_seq":0,
		"binlog_gap_suspected":false,
		"last_error":"[normal] bad state","last_error_category":"normal","last_error_at":3000,
		"first_failure_at":3000,"consecutive_failures":1,"last_success_at":2000,"panicked":true,
		"recent_errors":[
			{"at":3000,"category":"normal","message":"[normal] bad state","count":1},
			{"at":1000,"category":"meta","message":"[meta] table not found","count":1}]}`, string(data))

	// The stats are never persisted with the job info.
	data, err = json.Marshal(job)
	require.NoError(t, err)
	require.NotContains(t, string(data), "last_error")
}

// newRunTestJob builds a DB sync job whose sync() fails with a normal error
// for an unknown sync state, and returns nil for DBSpecificTableFullSync.
func newRunTestJob(state SyncState) *Job {
	return &Job{
		Name:     "run_job",
		SyncType: DBSync,
		State:    JobRunning,
		Extra:    JobExtra{InterruptCh: make(chan any, 1)},
		progress: &JobProgress{SyncState: state, SubSyncState: Done},
		stop:     make(chan struct{}),
	}
}

func TestJobRunRecordsFailuresAndSuccess(t *testing.T) {
	job := newRunTestJob(SyncState(-1))
	done := make(chan struct{})
	go func() {
		defer close(done)
		job.run()
	}()
	defer func() {
		job.Stop()
		<-done
	}()

	// SyncDuration is 3s, so allow a couple of ticks.
	require.Eventually(t, func() bool { return job.Status().ConsecutiveFailures >= 1 }, 8*time.Second, 50*time.Millisecond)
	status := job.Status()
	require.Equal(t, "normal", status.LastErrorCategory)
	require.Contains(t, status.LastError, "unknown db sync state")
	require.NotZero(t, status.FirstFailureAt)
	require.False(t, status.Panicked)

	job.lock.Lock()
	job.progress.SyncState = DBSpecificTableFullSync
	job.lock.Unlock()
	require.Eventually(t, func() bool { return job.Status().LastSuccessAt != 0 }, 8*time.Second, 50*time.Millisecond)
	status = job.Status()
	require.Zero(t, status.ConsecutiveFailures)
	require.Zero(t, status.FirstFailureAt)
	require.Contains(t, status.LastError, "unknown db sync state")
}
