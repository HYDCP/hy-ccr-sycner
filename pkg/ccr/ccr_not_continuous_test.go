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
	"reflect"
	"strings"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

func ccrSource(targetId, srcId int64) *festruct.TCcrPartitionSource {
	src := festruct.NewTCcrPartitionSource()
	src.TargetPartitionId = &targetId
	src.SrcPartitionId = &srcId
	return src
}

func ccrTableRecord(tableId int64, partitionIds ...int64) *record.TableRecord {
	tableRecord := &record.TableRecord{Id: tableId}
	for _, id := range partitionIds {
		tableRecord.PartitionRecords = append(tableRecord.PartitionRecords, record.PartitionRecord{Id: id})
	}
	return tableRecord
}

func TestPlanCcrResync(t *testing.T) {
	sources := []*festruct.TCcrPartitionSource{
		ccrSource(1001, 11), ccrSource(1002, 12), ccrSource(1003, 13), ccrSource(2001, 21), ccrSource(3001, 31),
	}
	tableRecords := []*record.TableRecord{ccrTableRecord(100, 11, 12, 13), ccrTableRecord(200, 21)}

	tests := []struct {
		name          string
		notContinuous []int64
		expect        *ccrResync
	}{
		{
			name:          "the partitions of one table",
			notContinuous: []int64{1003, 1001, 1002},
			expect:        &ccrResync{TableId: 100, DestPartitionIds: []int64{1003, 1001, 1002}},
		},
		{
			name:          "the partitions of two tables",
			notContinuous: []int64{1001, 2001},
		},
		{
			name:          "a target partition not in the sources",
			notContinuous: []int64{1001, 4001},
		},
		{
			name:          "a source partition not in the table records",
			notContinuous: []int64{3001},
		},
	}
	for _, test := range tests {
		resync := planCcrResync(test.notContinuous, sources, tableRecords)
		if test.expect == nil {
			if resync.FullSyncInfo == "" || resync.TableId != 0 || len(resync.DestPartitionIds) != 0 {
				t.Errorf("%s: expect a full sync, got %+v", test.name, resync)
			}
		} else if !reflect.DeepEqual(resync, test.expect) {
			t.Errorf("%s: expect %+v, got %+v", test.name, test.expect, resync)
		}
	}
}

func TestCcrResyncSnapshotParams(t *testing.T) {
	partitionNames := map[int64]string{1001: "p2", 1002: "p1", 1003: "p1"}
	partitionName := func(partitionId int64) (string, error) {
		if name, ok := partitionNames[partitionId]; ok {
			return name, nil
		}
		return "", xerror.Errorf(xerror.Meta, "partitionId %d not found", partitionId)
	}
	connectFailed := xerror.Errorf(xerror.Normal, "connect failed")

	tests := []struct {
		name          string
		partitionIds  []int64
		partitionName func(int64) (string, error)
		expect        *PartialSnapshotParams
		err           bool
	}{
		{
			name:          "the partitions",
			partitionIds:  []int64{1003, 1001, 1002},
			partitionName: partitionName,
			expect:        &PartialSnapshotParams{TableId: 100, TableName: "t1", Partitions: []string{"p1", "p2"}},
		},
		{
			name:          "a target partition not found",
			partitionIds:  []int64{1001, 1004},
			partitionName: partitionName,
			expect:        &PartialSnapshotParams{TableId: 100, TableName: "t1", Replace: true},
		},
		{
			// retried on the next tick, not a sync of the whole table
			name:          "a partition cannot be named now",
			partitionIds:  []int64{1001},
			partitionName: func(int64) (string, error) { return "", connectFailed },
			err:           true,
		},
	}
	for _, test := range tests {
		resync := &ccrResync{TableId: 100, DestPartitionIds: test.partitionIds}
		params, err := ccrResyncSnapshotParams(resync, "t1", test.partitionName)
		if test.err {
			if err == nil || params != nil {
				t.Errorf("%s: expect an error, got %+v, %v", test.name, params, err)
			}
		} else if err != nil || !reflect.DeepEqual(params, test.expect) {
			t.Errorf("%s: expect %+v, got %+v, %v", test.name, test.expect, params, err)
		}
	}
}

func TestCcrResyncPersisted(t *testing.T) {
	progress := &JobProgress{JobName: "job", CcrResync: &ccrResync{TableId: 100, DestPartitionIds: []int64{1001, 1002}}}
	data, err := json.Marshal(progress)
	if err != nil {
		t.Fatalf("marshal the progress: %v", err)
	}
	var decoded JobProgress
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal the progress: %v", err)
	}
	if !reflect.DeepEqual(decoded.CcrResync, progress.CcrResync) {
		t.Errorf("expect the resync %+v, got %+v", progress.CcrResync, decoded.CcrResync)
	}

	progress.CcrResync = &ccrResync{FullSyncInfo: "the partitions are of more than one source table"}
	data, _ = json.Marshal(progress)
	decoded = JobProgress{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal the progress: %v", err)
	}
	if !reflect.DeepEqual(decoded.CcrResync, progress.CcrResync) {
		t.Errorf("expect the resync %+v, got %+v", progress.CcrResync, decoded.CcrResync)
	}

	// no pending resync is not written, as an older syncer writes the progress
	progress.CcrResync = nil
	data, _ = json.Marshal(progress)
	if strings.Contains(string(data), "ccr_resync") {
		t.Errorf("expect no ccr_resync, got %s", data)
	}

	// the progress of an older syncer
	old := `{"job_name":"job","sync_state":1,"sub_sync_state":{"state":-1,"binlog_type":-1},"commit_seq":10,"data":""}`
	decoded = JobProgress{}
	if err := json.Unmarshal([]byte(old), &decoded); err != nil {
		t.Fatalf("unmarshal the old progress: %v", err)
	}
	if decoded.CcrResync != nil {
		t.Errorf("expect no pending resync, got %+v", decoded.CcrResync)
	}
}

func TestAddCcrPartitionSource(t *testing.T) {
	job := &IngestBinlogJob{}
	job.addCcrPartitionSource(1001, record.PartitionRecord{Id: 11, Stid: 5, PrevToken: "a", Token: "b"})
	job.addCcrPartitionSource(1002, record.PartitionRecord{Id: 12, PrevToken: "a"})
	job.addCcrPartitionSource(1003, record.PartitionRecord{Id: 13, Token: "b"})
	job.addCcrPartitionSource(1004, record.PartitionRecord{Id: 14, Version: 7, PrevToken: "a", Token: "b"})

	sources := job.CcrPartitionSources()
	if len(sources) != 1 {
		t.Fatalf("expect only the partition with both tokens and no sub txn, got %v", sources)
	}
	src := sources[0]
	if src.GetTargetPartitionId() != 1004 || src.GetSrcPartitionId() != 14 || src.GetSrcVersion() != 7 ||
		src.GetPrevToken() != "a" || src.GetToken() != "b" {
		t.Errorf("unexpected ccr partition source %v", src)
	}
}

func TestPlanCcrNotContinuousResyncNothingTold(t *testing.T) {
	j := &Job{SyncType: TableSync}
	// an older FE never sets the field
	resp := festruct.NewTCommitTxnResult_()
	if resync := j.planCcrNotContinuousResync(1, 1, resp.GetCcrNotContinuousPartitionIds(), nil, nil); resync != nil {
		t.Errorf("expect no resync, got %+v", resync)
	}

	old := *resyncNotContinuousPartitions
	*resyncNotContinuousPartitions = false
	defer func() { *resyncNotContinuousPartitions = old }()
	if resync := j.planCcrNotContinuousResync(1, 1, []int64{1001}, nil, nil); resync != nil {
		t.Errorf("expect no resync when switched off, got %+v", resync)
	}

	// switched on, the plan only maps the ids and asks nothing of the source
	*resyncNotContinuousPartitions = true
	sources := []*festruct.TCcrPartitionSource{ccrSource(1001, 11)}
	tableRecords := []*record.TableRecord{ccrTableRecord(100, 11)}
	expect := &ccrResync{TableId: 100, DestPartitionIds: []int64{1001}}
	if resync := j.planCcrNotContinuousResync(1, 1, []int64{1001}, sources, tableRecords); !reflect.DeepEqual(
		resync, expect) {
		t.Errorf("expect %+v, got %+v", expect, resync)
	}
}

func TestCommitTxnResultCcrNotContinuous(t *testing.T) {
	status := tstatus.NewTStatus()
	status.StatusCode = tstatus.TStatusCode_OK

	result := festruct.NewTCommitTxnResult_()
	result.Status = status
	result.CcrNotContinuousPartitionIds = []int64{1001, 1002}
	buf := make([]byte, result.BLength())
	result.FastWriteNocopy(buf, nil)
	decoded := festruct.NewTCommitTxnResult_()
	if _, err := decoded.FastRead(buf); err != nil {
		t.Fatalf("read the result: %v", err)
	}
	if !reflect.DeepEqual(decoded.GetCcrNotContinuousPartitionIds(), []int64{1001, 1002}) {
		t.Errorf("expect the not continuous partitions, got %v", decoded.GetCcrNotContinuousPartitionIds())
	}

	// the result of an older FE
	old := festruct.NewTCommitTxnResult_()
	old.Status = status
	buf = make([]byte, old.BLength())
	old.FastWriteNocopy(buf, nil)
	decoded = festruct.NewTCommitTxnResult_()
	if _, err := decoded.FastRead(buf); err != nil {
		t.Fatalf("read the result: %v", err)
	}
	if decoded.IsSetCcrNotContinuousPartitionIds() || len(decoded.GetCcrNotContinuousPartitionIds()) != 0 {
		t.Errorf("expect no not continuous partitions, got %v", decoded.GetCcrNotContinuousPartitionIds())
	}
}
