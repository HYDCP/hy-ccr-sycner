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
	"sort"
	"time"
)

const (
	// JobLagCollectInterval is how often the job collector computes the lags.
	JobLagCollectInterval = 10 * time.Second

	jobLagsMessageMaxChars = 200
)

// JobLag is the lag of a job from one job collector round, computed against
// the commit seq persisted in the job progress, as /get_lag does.
type JobLag struct {
	Lag                   int64
	NextBinlogTimestampMs int64 // the oldest unsynced binlog, 0 when unknown
	CommitSeq             int64
	ComputedAt            int64  // unix ms, 0 when never computed
	Error                 string // why the latest round failed, the other fields are from the last success
}

// JobLagSnapshot is the result of one job collector round. It is published as
// a whole and must not be modified afterwards.
type JobLagSnapshot struct {
	GeneratedAt int64 // unix ms
	Jobs        map[string]JobLag
}

// JobLagEntry is one job of GET /job_lags. Time fields are unix epoch
// milliseconds; the lag fields are null until the collector computed them.
type JobLagEntry struct {
	Name                  string `json:"name"`
	Lag                   *int64 `json:"lag"`
	NextBinlogTimestampMs *int64 `json:"next_binlog_timestamp_ms"`
	CommitSeq             *int64 `json:"commit_seq"`
	ComputedAt            *int64 `json:"computed_at"`
	LagError              string `json:"lag_error,omitempty"`

	LastErrorCategory   string `json:"last_error_category,omitempty"`
	LastErrorAt         int64  `json:"last_error_at,omitempty"`
	FirstFailureAt      int64  `json:"first_failure_at,omitempty"`
	ConsecutiveFailures int64  `json:"consecutive_failures"`
	LastSuccessAt       int64  `json:"last_success_at,omitempty"`
	LastError           string `json:"last_error,omitempty"` // the first 200 characters
	Panicked            bool   `json:"panicked"`
	BinlogGapSuspected  bool   `json:"binlog_gap_suspected"`
}

// BuildJobLagEntries joins the jobs with the lag snapshot (may be nil) and the
// error summary of each job. It only reads the snapshot and the per-job
// atomic/small-lock fields.
func BuildJobLagEntries(jobs []*Job, snapshot *JobLagSnapshot) []JobLagEntry {
	entries := make([]JobLagEntry, 0, len(jobs))
	for _, job := range jobs {
		if job.IsDeleted() {
			continue
		}

		entry := JobLagEntry{Name: job.Name}
		if snapshot != nil {
			if lag, ok := snapshot.Jobs[job.Name]; ok {
				if lag.ComputedAt != 0 {
					entry.Lag, entry.CommitSeq, entry.ComputedAt = &lag.Lag, &lag.CommitSeq, &lag.ComputedAt
					if lag.NextBinlogTimestampMs > 0 {
						entry.NextBinlogTimestampMs = &lag.NextBinlogTimestampMs
					}
				}
				entry.LagError = truncateChars(lag.Error, jobLagsMessageMaxChars)
			}
		}

		stats := job.errStats.get()
		entry.LastErrorCategory = stats.LastErrorCategory
		entry.LastErrorAt = stats.LastErrorAt
		entry.FirstFailureAt = stats.FirstFailureAt
		entry.ConsecutiveFailures = stats.ConsecutiveFailures
		entry.LastSuccessAt = stats.LastSuccessAt
		entry.LastError = truncateChars(stats.LastError, jobLagsMessageMaxChars)
		entry.Panicked = stats.Panicked
		entry.BinlogGapSuspected = job.binlogGap.get().BinlogGapSuspected
		entries = append(entries, entry)
	}
	return entries
}

// PublishJobLagSnapshot replaces the lag snapshot read by JobLagSnapshot.
func (jm *JobManager) PublishJobLagSnapshot(snapshot *JobLagSnapshot) {
	jm.lagSnapshot.Store(snapshot)
}

// JobLagSnapshot returns the latest lag snapshot, nil before the first round.
func (jm *JobManager) JobLagSnapshot() *JobLagSnapshot {
	return jm.lagSnapshot.Load()
}

// JobsView returns the jobs of this node sorted by name, without taking the
// job manager lock, which AddJob holds while it calls FE. Read only.
func (jm *JobManager) JobsView() []*Job {
	if view := jm.jobsView.Load(); view != nil {
		return *view
	}
	return nil
}

// refreshJobsViewLocked rebuilds the JobsView copy, call it with jm.lock held
// after jm.jobs changed.
func (jm *JobManager) refreshJobsViewLocked() {
	view := make([]*Job, 0, len(jm.jobs))
	for _, job := range jm.jobs {
		view = append(view, job)
	}
	sort.Slice(view, func(i, k int) bool { return view[i].Name < view[k].Name })
	jm.jobsView.Store(&view)
}

// truncateChars returns the first maxChars characters of s.
func truncateChars(s string, maxChars int) string {
	chars := 0
	for i := range s {
		if chars == maxChars {
			return s[:i]
		}
		chars++
	}
	return s
}
