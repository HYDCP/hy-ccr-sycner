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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
	"github.com/stretchr/testify/require"
)

func TestBuildJobLagEntries(t *testing.T) {
	computed := &Job{Name: "a_computed"}
	computed.errStats.recordSuccess(time.UnixMilli(2_000))
	computed.errStats.recordFailure(xerror.Errorf(xerror.RPC, "connect fe timeout"), false, time.UnixMilli(5_000))
	computed.errStats.recordFailure(xerror.Errorf(xerror.RPC, "connect fe timeout"), false, time.UnixMilli(8_000))
	computed.binlogGap.mark(41, time.UnixMilli(4_000))

	caughtUp := &Job{Name: "b_caught_up"}
	caughtUp.errStats.recordFailure(xerror.Panicf(xerror.Normal, "%s", strings.Repeat("错", 300)), true, time.UnixMilli(6_000))

	neverComputed := &Job{Name: "c_never_computed"}
	notCollected := &Job{Name: "d_not_collected"}
	notCollected.errStats.recordSuccess(time.UnixMilli(9_500))
	deleted := &Job{Name: "e_deleted"}
	deleted.isDeleted.Store(true)

	snapshot := &JobLagSnapshot{GeneratedAt: 9_000, Jobs: map[string]JobLag{
		// The latest round failed, so the lag is the last computed one plus the error.
		"a_computed": {Lag: 7, NextBinlogTimestampMs: 1_000, CommitSeq: 42, ComputedAt: 3_000, Error: "rpc get bin log failed"},
		// FE reports no next binlog as -1 (or 0): no timestamp, never the 1970 placeholder.
		"b_caught_up":      {Lag: 0, NextBinlogTimestampMs: -1, CommitSeq: 99, ComputedAt: 9_000},
		"c_never_computed": {Error: "get binlog lag failed, status: BINLOG_NOT_FOUND_DB"},
		"e_deleted":        {Lag: 1, ComputedAt: 9_000},
	}}

	entries := BuildJobLagEntries([]*Job{computed, caughtUp, neverComputed, notCollected, deleted}, snapshot)
	data, err := json.Marshal(entries)
	require.NoError(t, err)
	require.JSONEq(t, `[
		{"name":"a_computed","lag":7,"next_binlog_timestamp_ms":1000,"commit_seq":42,"computed_at":3000,
		 "lag_error":"rpc get bin log failed",
		 "last_error_category":"rpc","last_error_at":8000,"first_failure_at":5000,"consecutive_failures":2,
		 "last_success_at":2000,"last_error":"[rpc] connect fe timeout",
		 "panicked":false,"binlog_gap_suspected":true},
		{"name":"b_caught_up","lag":0,"next_binlog_timestamp_ms":null,"commit_seq":99,"computed_at":9000,
		 "last_error_category":"normal","last_error_at":6000,"first_failure_at":6000,"consecutive_failures":1,
		 "last_error":"[normal] `+strings.Repeat("错", 191)+`",
		 "panicked":true,"binlog_gap_suspected":false},
		{"name":"c_never_computed","lag":null,"next_binlog_timestamp_ms":null,"commit_seq":null,"computed_at":null,
		 "lag_error":"get binlog lag failed, status: BINLOG_NOT_FOUND_DB",
		 "consecutive_failures":0,"panicked":false,"binlog_gap_suspected":false},
		{"name":"d_not_collected","lag":null,"next_binlog_timestamp_ms":null,"commit_seq":null,"computed_at":null,
		 "consecutive_failures":0,"last_success_at":9500,"panicked":false,"binlog_gap_suspected":false}
	]`, string(data))
	require.Equal(t, 200, utf8.RuneCountInString(entries[1].LastError))

	// Before the first collector round there is no snapshot at all.
	entries = BuildJobLagEntries([]*Job{computed}, nil)
	require.Len(t, entries, 1)
	require.Nil(t, entries[0].Lag)
	require.Equal(t, int64(2), entries[0].ConsecutiveFailures)

	data, err = json.Marshal(BuildJobLagEntries(nil, snapshot))
	require.NoError(t, err)
	require.Equal(t, "[]", string(data))
}

func TestJobsViewDoesNotWaitForJobManagerLock(t *testing.T) {
	jm := NewJobManager(nil, nil, "test-syncer")
	require.Nil(t, jm.JobsView())
	require.Nil(t, jm.JobLagSnapshot())

	jm.lock.Lock()
	for _, name := range []string{"job_c", "job_a", "job_b"} {
		jm.jobs[name] = &Job{Name: name}
	}
	jm.refreshJobsViewLocked()

	// AddJob holds the write lock while it calls FE; the readers must not wait.
	done := make(chan []JobLagEntry)
	go func() {
		jm.PublishJobLagSnapshot(&JobLagSnapshot{GeneratedAt: 1, Jobs: map[string]JobLag{"job_b": {Lag: 3, ComputedAt: 1}}})
		done <- BuildJobLagEntries(jm.JobsView(), jm.JobLagSnapshot())
	}()
	var entries []JobLagEntry
	select {
	case entries = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("job lags waited for the job manager lock")
	}
	jm.lock.Unlock()

	require.Len(t, entries, 3)
	for i, name := range []string{"job_a", "job_b", "job_c"} {
		require.Equal(t, name, entries[i].Name)
	}
	require.Equal(t, int64(3), *entries[1].Lag)
}

func TestJobLagsConcurrentPublishAndRead(t *testing.T) {
	jm := NewJobManager(nil, nil, "test-syncer")
	jobs := make([]*Job, 0, 20)
	for i := 0; i < 20; i++ {
		job := &Job{Name: fmt.Sprintf("job_%02d", i)}
		jobs = append(jobs, job)
		jm.jobs[job.Name] = job
	}
	jm.refreshJobsViewLocked()

	stop := make(chan struct{})
	var writers, readers sync.WaitGroup
	writers.Add(3)
	go func() { // the collector: build a whole new snapshot, then publish it
		defer writers.Done()
		for round := int64(1); round <= 200; round++ {
			lags := make(map[string]JobLag, len(jobs))
			for _, job := range jobs {
				lags[job.Name] = JobLag{Lag: round, CommitSeq: round, ComputedAt: round}
			}
			jm.PublishJobLagSnapshot(&JobLagSnapshot{GeneratedAt: round, Jobs: lags})
		}
	}()
	go func() { // the job routines
		defer writers.Done()
		for i := 0; i < 200; i++ {
			job := jobs[i%len(jobs)]
			job.errStats.recordFailure(fmt.Errorf("error %d", i), false, time.Now())
			job.binlogGap.mark(int64(i), time.Now())
			job.errStats.recordSuccess(time.Now())
		}
	}()
	go func() { // jobs added and removed under the job manager lock
		defer writers.Done()
		for i := 0; i < 200; i++ {
			name := fmt.Sprintf("extra_%d", i%3)
			jm.lock.Lock()
			if _, ok := jm.jobs[name]; ok {
				delete(jm.jobs, name)
			} else {
				jm.jobs[name] = &Job{Name: name}
			}
			jm.refreshJobsViewLocked()
			jm.lock.Unlock()
		}
	}()
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snapshot := jm.JobLagSnapshot()
				entries := BuildJobLagEntries(jm.JobsView(), snapshot)
				if snapshot == nil {
					continue
				}
				// A snapshot is never seen half built: every job has the same round.
				for _, entry := range entries {
					if strings.HasPrefix(entry.Name, "job_") && (entry.Lag == nil || *entry.Lag != snapshot.GeneratedAt) {
						panic(fmt.Sprintf("job %s has a lag from another round than %d", entry.Name, snapshot.GeneratedAt))
					}
				}
				_, _ = json.Marshal(entries)
			}
		}()
	}
	writers.Wait()
	close(stop)
	readers.Wait()
	require.Equal(t, int64(200), jm.JobLagSnapshot().GeneratedAt)
}
