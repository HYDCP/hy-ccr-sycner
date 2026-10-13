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
	"errors"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

const (
	jobErrorMessageMaxBytes = 1024
	jobRecentErrorsMax      = 10
	// The category of errors that are not an xerror, e.g. a recovered panic.
	jobErrorCategoryUnknown = "unknown"
)

// JobErrorRecord is one entry of JobErrorStatus.RecentErrors. Consecutive
// failures (no success in between) with the same category and message share
// one entry, so the 10 entries reach further back.
type JobErrorRecord struct {
	At       int64  `json:"at"` // the latest occurrence, unix epoch milliseconds
	Category string `json:"category"`
	Message  string `json:"message"` // truncated to 1KB
	Count    int64  `json:"count"`   // consecutive occurrences
}

// JobErrorStatus is the error part of JobStatus. The time fields are unix
// epoch milliseconds, 0 (omitted) means never.
type JobErrorStatus struct {
	// The latest error, kept after the job recovers; truncated to 1KB.
	LastError         string `json:"last_error,omitempty"`
	LastErrorCategory string `json:"last_error_category,omitempty"`
	LastErrorAt       int64  `json:"last_error_at,omitempty"`
	// The first failure of the current streak, 0 when the streak is 0.
	FirstFailureAt int64 `json:"first_failure_at,omitempty"`
	// The failed rounds since the last success.
	ConsecutiveFailures int64 `json:"consecutive_failures"`
	// The last sync round that returned no error, or the last advance of the
	// synced commit seq. It does not mean that any data was synced: an idle
	// job succeeds every round.
	LastSuccessAt int64 `json:"last_success_at,omitempty"`
	// The job routine has stopped syncing (a panic, or Job.Run returned an
	// error) and only a restart of the syncer process recovers it.
	Panicked bool `json:"panicked"`
	// At most 10 entries, newest first.
	RecentErrors []JobErrorRecord `json:"recent_errors,omitempty"`
}

// jobErrorStats tracks the errors of a job's sync rounds. It is observation
// only, in memory, and has its own lock so readers never wait for the job or
// the job manager.
type jobErrorStats struct {
	mu     sync.Mutex
	status JobErrorStatus

	// The last synced commit seq seen, an advance counts as a success.
	commitSeq      int64
	commitSeqKnown bool
}

func (s *jobErrorStats) recordFailure(err error, panicked bool, now time.Time) {
	record := JobErrorRecord{
		At:       now.UnixMilli(),
		Category: jobErrorCategory(err),
		Message:  truncateUTF8(err.Error(), jobErrorMessageMaxBytes),
		Count:    1,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	status := &s.status
	inStreak := status.ConsecutiveFailures > 0
	status.LastError, status.LastErrorCategory, status.LastErrorAt = record.Message, record.Category, record.At
	if !inStreak {
		status.FirstFailureAt = record.At
	}
	status.ConsecutiveFailures++
	if panicked {
		status.Panicked = true
	}

	// get() hands out copies, so the entries can be changed in place.
	recent := status.RecentErrors
	if inStreak && len(recent) > 0 && recent[0].Category == record.Category && recent[0].Message == record.Message {
		recent[0].At = record.At
		recent[0].Count++
		return
	}
	if len(recent) < jobRecentErrorsMax {
		recent = append(recent, JobErrorRecord{})
	}
	copy(recent[1:], recent[:len(recent)-1])
	recent[0] = record
	status.RecentErrors = recent
}

func (s *jobErrorStats) recordSuccess(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markSuccessLocked(now)
}

// observeCommitSeq counts an advance of the synced commit seq as a success, so
// a long round that keeps syncing binlogs is not shown as still failing.
func (s *jobErrorStats) observeCommitSeq(commitSeq int64, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.commitSeqKnown && commitSeq > s.commitSeq {
		s.markSuccessLocked(now)
	}
	s.commitSeq, s.commitSeqKnown = commitSeq, true
}

func (s *jobErrorStats) markSuccessLocked(now time.Time) {
	s.status.LastSuccessAt = now.UnixMilli()
	s.status.ConsecutiveFailures = 0
	s.status.FirstFailureAt = 0
}

func (s *jobErrorStats) get() JobErrorStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	status := s.status
	status.RecentErrors = append([]JobErrorRecord(nil), s.status.RecentErrors...)
	return status
}

// jobErrorCategory returns the category handleError acts on: the outermost xerror.
func jobErrorCategory(err error) string {
	var xerr *xerror.XError
	if errors.As(err, &xerr) {
		return xerr.Category().Name()
	}
	return jobErrorCategoryUnknown
}

// truncateUTF8 cuts s to at most maxBytes without splitting a UTF-8 character.
func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	for maxBytes > 0 && !utf8.RuneStart(s[maxBytes]) {
		maxBytes--
	}
	return s[:maxBytes]
}
