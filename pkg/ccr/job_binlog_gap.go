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
	"sync"
	"time"

	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	utils "github.com/selectdb/ccr_syncer/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// BinlogGapStatus is the suspected binlog gap part of JobStatus. The time
// field is unix epoch milliseconds, 0 (omitted) means never.
type BinlogGapStatus struct {
	// The upstream answered BINLOG_TOO_OLD_COMMIT_SEQ: the binlogs after the
	// synced commit seq may have been recycled before they were synced.
	BinlogGapSuspected bool `json:"binlog_gap_suspected"`
	// When the gap was first seen, and the commit seq requested then. Later
	// answers are only logged.
	BinlogGapFirstSeenAt int64 `json:"binlog_gap_first_seen_at,omitempty"`
	BinlogGapCommitSeq   int64 `json:"binlog_gap_commit_seq,omitempty"`
}

// binlogGapMarker remembers BINLOG_TOO_OLD_COMMIT_SEQ answers for a job. It is
// observation only, in memory, and has its own lock so readers never wait for
// the job or the job manager.
type binlogGapMarker struct {
	mu     sync.Mutex
	status BinlogGapStatus
}

func (m *binlogGapMarker) mark(commitSeq int64, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.status.BinlogGapSuspected {
		m.status = BinlogGapStatus{
			BinlogGapSuspected:   true,
			BinlogGapFirstSeenAt: now.UnixMilli(),
			BinlogGapCommitSeq:   commitSeq,
		}
	}
}

// clear resets the marker and reports whether it was set.
func (m *binlogGapMarker) clear() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	suspected := m.status.BinlogGapSuspected
	m.status = BinlogGapStatus{}
	return suspected
}

func (m *binlogGapMarker) get() BinlogGapStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

// reportBinlogTooOld logs and marks a BINLOG_TOO_OLD_COMMIT_SEQ answer of
// GetBinlog(prevCommitSeq). The caller keeps handling the response as before.
func (j *Job) reportBinlogTooOld(source string, prevCommitSeq int64, resp *festruct.TGetBinlogResult_) {
	firstCommitSeq, firstType := int64(-1), "none"
	if binlogs := resp.GetBinlogs(); len(binlogs) > 0 && binlogs[0] != nil {
		firstCommitSeq, firstType = binlogs[0].GetCommitSeq(), binlogs[0].GetType().String()
	}
	log.Errorf("binlog too old, the upstream may have recycled the binlogs after commit seq %d before they were synced, "+
		"data may be missing downstream. job: %s, source: %s, first returned binlog commit seq: %d, type: %s, msg: %s",
		prevCommitSeq, j.Name, source, firstCommitSeq, firstType, utils.FirstOr(resp.GetStatus().GetErrorMsgs(), ""))
	j.binlogGap.mark(prevCommitSeq, time.Now())
}

// clearBinlogGapIfFullSynced clears the gap marker once a full sync has
// finished, because the full sync rebuilt the downstream from a new snapshot.
// A partial sync, even of a whole table, does not clear it (conservative).
func (j *Job) clearBinlogGapIfFullSynced() {
	if j.progress.SyncState == TableFullSync || j.progress.SyncState == DBFullSync {
		return
	}
	if j.binlogGap.clear() {
		log.Infof("full sync done, clear the suspected binlog gap, job: %s", j.Name)
	}
}
