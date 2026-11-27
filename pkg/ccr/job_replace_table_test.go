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
	"path/filepath"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A table sync job must persist the new src table id as soon as it handles a REPLACE_TABLE
// binlog, so that readers of the stored job info (get_lag, job_list, metrics collector) do not
// query the dropped table id, and a syncer restart during the following full sync does not
// trigger another full sync.
func TestHandleReplaceTablePersistsNewSrcTableIdInTableSync(t *testing.T) {
	const (
		jobName       = "replace_table_job"
		originTableId = int64(1001)
		newTableId    = int64(2002)
		commitSeq     = int64(300)
	)

	db, err := storage.NewSQLiteDB(filepath.Join(t.TempDir(), "ccr.db"))
	require.NoError(t, err)

	job := &Job{
		Name:     jobName,
		SyncType: TableSync,
		Src:      base.Spec{Database: "src_db", Table: "tbl", TableId: originTableId},
		Dest:     base.Spec{Database: "dest_db", Table: "tbl", TableId: 3003},
		State:    JobRunning,
		db:       db,
	}
	data, err := json.Marshal(job)
	require.NoError(t, err)
	require.NoError(t, db.AddJob(jobName, string(data), "127.0.0.1:9190"))

	job.progress = NewJobProgress(jobName, TableSync, db)
	job.progress.NextWithPersist(commitSeq-1, TableIncrementalSync, Done, "")

	recordData := `{"dbId":1,"origTblId":1001,"origTblName":"tbl","newTblName":2002,"actualNewTblName":"tbl_tmp","swapTable":false,"isForce":false}`
	binlog := festruct.NewTBinlog()
	seq := commitSeq
	binlog.CommitSeq = &seq
	binlog.Data = &recordData

	require.NoError(t, job.handleReplaceTable(binlog))
	require.Equal(t, newTableId, job.Src.TableId)
	require.Equal(t, TableFullSync, job.progress.SyncState)

	stored, err := db.GetJobInfo(jobName)
	require.NoError(t, err)
	var storedJob Job
	require.NoError(t, json.Unmarshal([]byte(stored), &storedJob))
	require.Equal(t, newTableId, storedJob.Src.TableId)
}
