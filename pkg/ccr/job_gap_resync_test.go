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
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

// fakeMetaer implements Metaer, only UpdateTable is stubbed, any other call panics.
type fakeMetaer struct {
	Metaer
	updateTable func(tableName string, tableId int64) (*TableMeta, error)
}

func (f *fakeMetaer) UpdateTable(tableName string, tableId int64) (*TableMeta, error) {
	return f.updateTable(tableName, tableId)
}

func TestCheckIntactBeforeGapResync(t *testing.T) {
	metaNotFound := xerror.New(xerror.Meta, "table not found")

	tests := []struct {
		name        string
		updateTable func(tableName string, tableId int64) (*TableMeta, error)
		expectErr   string // empty means no error
	}{
		{
			name: "table id intact and name matches",
			updateTable: func(tableName string, tableId int64) (*TableMeta, error) {
				return &TableMeta{Id: 1, Name: "t"}, nil
			},
			expectErr: "",
		},
		{
			name: "table id intact but renamed or id reused",
			updateTable: func(tableName string, tableId int64) (*TableMeta, error) {
				return &TableMeta{Id: 1, Name: "t_old"}, nil
			},
			expectErr: "renamed or id reused",
		},
		{
			name: "table id gone and name gone, dropped",
			updateTable: func(tableName string, tableId int64) (*TableMeta, error) {
				return nil, metaNotFound
			},
			expectErr: "has been dropped",
		},
		{
			name: "table id gone but name exists, recreated",
			updateTable: func(tableName string, tableId int64) (*TableMeta, error) {
				if tableId != 0 {
					return nil, metaNotFound
				}
				return &TableMeta{Id: 2, Name: "t"}, nil
			},
			expectErr: "has been recreated",
		},
		{
			name: "check failed with non meta error",
			updateTable: func(tableName string, tableId int64) (*TableMeta, error) {
				return nil, xerror.New(xerror.Normal, "connection refused")
			},
			expectErr: "connection refused",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job := &Job{
				Name:     "test_gap_resync",
				SyncType: TableSync,
				Src:      base.Spec{Database: "db", Table: "t", TableId: 1},
				srcMeta:  &fakeMetaer{updateTable: test.updateTable},
			}

			err := job.checkIntactBeforeGapResync()
			if test.expectErr == "" {
				if err != nil {
					t.Fatalf("expect no error, but got: %+v", err)
				}
			} else if err == nil {
				t.Fatalf("expect error contains %q, but got nil", test.expectErr)
			} else if !strings.Contains(err.Error(), test.expectErr) {
				t.Fatalf("expect error contains %q, but got: %+v", test.expectErr, err)
			}
		})
	}
}

func TestCheckIntactBeforeGapResyncDBSync(t *testing.T) {
	job := &Job{
		Name:     "test_gap_resync_db",
		SyncType: DBSync,
		Src:      base.Spec{Database: "db"},
		srcMeta: &fakeMetaer{updateTable: func(tableName string, tableId int64) (*TableMeta, error) {
			t.Fatalf("update table should not be called for db sync")
			return nil, nil
		}},
	}

	if err := job.checkIntactBeforeGapResync(); err != nil {
		t.Fatalf("expect no error for db sync, but got: %+v", err)
	}
}

// fakeFeRpc implements rpc.IFeRpc, only RollbackTransaction is stubbed, any
// other call panics.
type fakeFeRpc struct {
	rpc.IFeRpc
	rolledBackTxnIds []int64
}

func (f *fakeFeRpc) RollbackTransaction(spec *base.Spec, txnId int64) (*festruct.TRollbackTxnResult_, error) {
	f.rolledBackTxnIds = append(f.rolledBackTxnIds, txnId)
	return &festruct.TRollbackTxnResult_{
		Status: &tstatus.TStatus{StatusCode: tstatus.TStatusCode_OK},
	}, nil
}

type fakeRpcFactory struct {
	feRpc rpc.IFeRpc
}

func (f *fakeRpcFactory) NewFeRpc(spec *base.Spec) (rpc.IFeRpc, error) {
	return f.feRpc, nil
}

func (f *fakeRpcFactory) NewBeRpc(be *base.Backend) (rpc.IBeRpc, error) {
	return nil, xerror.New(xerror.Normal, "not implemented")
}

// A binlog gap detected in the pipeline sync should rollback the inflight txns
// first, then trigger the full sync.
func TestPipelineGapResyncRollbackBeforeFullSync(t *testing.T) {
	db, err := storage.NewSQLiteDB(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("new sqlite db failed: %+v", err)
	}

	feRpc := &fakeFeRpc{}

	progress := NewJobProgress("test_pipeline_gap_resync", TableSync, db)
	progress.SyncState = TableIncrementalSync
	progress.SubSyncState = RollbackPipeline
	progress.CommitSeq = 1234
	progress.InMemoryData = &PipelineInMemoryData{
		RunningTxnList: []*TxnContext{{TxnId: 1001}, {TxnId: 1002}},
	}

	job := &Job{
		Name:     "test_pipeline_gap_resync",
		SyncType: TableSync,
		Src:      base.Spec{Database: "db", Table: "t", TableId: 1},
		Dest:     base.Spec{Database: "db", Table: "t", TableId: 2},
		Extra: JobExtra{
			BinlogGapResyncReason: "binlog gap: commit seq 1234 is older than the earliest binlog in upstream, trigger full sync",
		},
		factory:  &Factory{IRpcFactory: &fakeRpcFactory{feRpc: feRpc}},
		progress: progress,
	}

	if err := job.pipelineSync(); err != nil {
		t.Fatalf("pipeline sync failed: %+v", err)
	}

	// The inflight txns are rolled back before the full sync is triggered.
	if !reflect.DeepEqual(feRpc.rolledBackTxnIds, []int64{1001, 1002}) {
		t.Fatalf("expect rollback txn 1001 and 1002, but got: %v", feRpc.rolledBackTxnIds)
	}
	if progress.SyncState != TableFullSync || progress.SubSyncState != BeginCreateSnapshot {
		t.Fatalf("expect full sync state, but got: %s/%s", progress.SyncState, progress.SubSyncState)
	}
	if job.Extra.BinlogGapResyncReason != "" {
		t.Fatalf("expect the gap resync reason is cleared, but got: %s", job.Extra.BinlogGapResyncReason)
	}
}
