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

// State machine tests of the resync of the not continuous partitions (JobProgress.CcrResync): they drive the real
// startCcrResync, Job.sync (the entry a tick runs), incrementalSyncInternal, pipelineSync, handleUpsert and the
// JobProgress persistence against a real SQLite storage, with in-memory fakes of the target meta, the FE rpc and the
// thrift meta.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	festruct_types "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/types"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

const (
	resyncJobName     = "resync_state_job"
	resyncSrcTableId  = int64(1001)
	resyncDestTableId = int64(2001)
	resyncCommitSeq   = int64(100)
)

// ---------------------------------------------------------------- fakes

// resyncEnv records the calls of the fakes, in order.
type resyncEnv struct {
	mu     sync.Mutex
	events []string
}

func (e *resyncEnv) record(format string, args ...any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, fmt.Sprintf(format, args...))
}

func (e *resyncEnv) eventsWithPrefix(prefix string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var found []string
	for _, event := range e.events {
		if strings.HasPrefix(event, prefix) {
			found = append(found, event)
		}
	}
	return found
}

func (e *resyncEnv) indexOf(prefix string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, event := range e.events {
		if strings.HasPrefix(event, prefix) {
			return i
		}
	}
	return -1
}

// fakeResyncMeta is the target (or source) Metaer: only the lookups of a resync are implemented, any other call
// panics on the nil embedded interface, so an unexpected meta access fails the test.
type fakeResyncMeta struct {
	Metaer
	env  *resyncEnv
	side string

	mu             sync.Mutex
	tableNames     map[int64]string
	tableNameErr   error
	partitionNames map[int64]string
	partitionErrs  map[int64]error
}

func (m *fakeResyncMeta) GetTableNameById(tableId int64) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.env.record("%s.GetTableNameById(%d)", m.side, tableId)
	if m.tableNameErr != nil {
		return "", m.tableNameErr
	}
	return m.tableNames[tableId], nil
}

func (m *fakeResyncMeta) GetPartitionName(tableId, partitionId int64) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.env.record("%s.GetPartitionName(%d,%d)", m.side, tableId, partitionId)
	if err := m.partitionErrs[partitionId]; err != nil {
		return "", err
	}
	name, ok := m.partitionNames[partitionId]
	if !ok {
		return "", xerror.Errorf(xerror.Normal, "fake: unexpected partition %d", partitionId)
	}
	return name, nil
}

func (m *fakeResyncMeta) ClearTablesCache() {}

type fakeResyncMetaFactory struct {
	src, dest *fakeResyncMeta
}

func (f *fakeResyncMetaFactory) NewMeta(spec *base.Spec) Metaer {
	if spec.Host == "src" {
		return f.src
	}
	return f.dest
}

// fakeResyncFeRpc is the FE rpc of both clusters.
type fakeResyncFeRpc struct {
	rpc.IFeRpc
	env *resyncEnv

	mu            sync.Mutex
	commitResult  *festruct.TCommitTxnResult_
	commitSources [][]*festruct.TCcrPartitionSource
	getBinlog     func() (*festruct.TGetBinlogResult_, error)
}

func (f *fakeResyncFeRpc) CommitTransaction(spec *base.Spec, txnId int64, _ []*festruct_types.TTabletCommitInfo,
	_ bool, sources ...*festruct.TCcrPartitionSource) (*festruct.TCommitTxnResult_, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.env.record("rpc.CommitTransaction(%s,%d)", spec.Host, txnId)
	f.commitSources = append(f.commitSources, sources)
	return f.commitResult, nil
}

func (f *fakeResyncFeRpc) RollbackTransaction(spec *base.Spec, txnId int64) (*festruct.TRollbackTxnResult_, error) {
	f.env.record("rpc.RollbackTransaction(%s,%d)", spec.Host, txnId)
	resp := festruct.NewTRollbackTxnResult_()
	resp.Status = &tstatus.TStatus{StatusCode: tstatus.TStatusCode_OK}
	return resp, nil
}

func (f *fakeResyncFeRpc) GetBinlog(spec *base.Spec, commitSeq int64, _ int64) (*festruct.TGetBinlogResult_, error) {
	f.env.record("rpc.GetBinlog(%s,%d)", spec.Host, commitSeq)
	if f.getBinlog != nil {
		return f.getBinlog()
	}
	// nothing new: incrementalSyncInternal / getNextBinlogs return without waiting (PrevTxnId is -1)
	resp := festruct.NewTGetBinlogResult_()
	resp.Status = &tstatus.TStatus{StatusCode: tstatus.TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ}
	return resp, nil
}

func (f *fakeResyncFeRpc) LockBinlog(spec *base.Spec, _ string, _ int64, commitSeq int64) (*festruct.TLockBinlogResult_, error) {
	f.env.record("rpc.LockBinlog(%s,%d)", spec.Host, commitSeq)
	resp := festruct.NewTLockBinlogResult_()
	resp.Status = &tstatus.TStatus{StatusCode: tstatus.TStatusCode_OK}
	return resp, nil
}

type fakeResyncRpcFactory struct {
	fe *fakeResyncFeRpc
}

func (f *fakeResyncRpcFactory) NewFeRpc(*base.Spec) (rpc.IFeRpc, error) { return f.fe, nil }
func (f *fakeResyncRpcFactory) NewBeRpc(*base.Backend) (rpc.IBeRpc, error) {
	panic("fake: NewBeRpc is not expected")
}

// fakeResyncThriftMetaFactory answers the source table meta GetDestTableIdBySrc asks for a table not in the mapping.
type fakeResyncThriftMetaFactory struct {
	env           *resyncEnv
	droppedTables map[int64]struct{}
}

func (f *fakeResyncThriftMetaFactory) NewThriftMeta(spec *base.Spec, _ rpc.IRpcFactory, tableIds []int64) (*ThriftMeta, error) {
	f.env.record("thrift.NewThriftMeta(%s,%v)", spec.Host, tableIds)
	return &ThriftMeta{meta: NewMeta(spec), droppedTables: f.droppedTables}, nil
}

// recordingDB is the real SQLite storage, it also keeps every progress written, to check what was persisted together.
type recordingDB struct {
	storage.DB
	mu       sync.Mutex
	progress []string
}

func (d *recordingDB) UpdateProgress(jobName string, progress string) error {
	d.mu.Lock()
	d.progress = append(d.progress, progress)
	d.mu.Unlock()
	return d.DB.UpdateProgress(jobName, progress)
}

func (d *recordingDB) history() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.progress...)
}

// ---------------------------------------------------------------- fixture

type resyncFixture struct {
	t        *testing.T
	env      *resyncEnv
	db       *recordingDB
	factory  *Factory
	srcMeta  *fakeResyncMeta
	destMeta *fakeResyncMeta
	fe       *fakeResyncFeRpc
	thrift   *fakeResyncThriftMetaFactory
}

func newResyncFixture(t *testing.T) *resyncFixture {
	t.Helper()
	sqlite, err := storage.NewSQLiteDB(filepath.Join(t.TempDir(), "ccr.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	env := &resyncEnv{}
	f := &resyncFixture{
		t:   t,
		env: env,
		db:  &recordingDB{DB: sqlite},
		srcMeta: &fakeResyncMeta{env: env, side: "src",
			tableNames: map[int64]string{}, partitionNames: map[int64]string{}, partitionErrs: map[int64]error{}},
		destMeta: &fakeResyncMeta{env: env, side: "dest",
			tableNames: map[int64]string{}, partitionNames: map[int64]string{}, partitionErrs: map[int64]error{}},
		fe:     &fakeResyncFeRpc{env: env},
		thrift: &fakeResyncThriftMetaFactory{env: env},
	}
	f.factory = NewFactory(&fakeResyncRpcFactory{fe: f.fe},
		&fakeResyncMetaFactory{src: f.srcMeta, dest: f.destMeta}, base.NewSpecerFactory(), f.thrift)
	return f
}

// newJob makes a job in the incremental sync, done at resyncCommitSeq, as after a commit whose result told the not
// continuous partitions: the progress (with the given resync) is persisted.
func (f *resyncFixture) newJob(syncType SyncType, srcTable, destTable string, resync *ccrResync) *Job {
	f.t.Helper()
	src := base.Spec{Frontend: base.Frontend{Host: "src", Port: "9030", ThriftPort: "9020"}, Database: "src_db"}
	dest := base.Spec{Frontend: base.Frontend{Host: "dest", Port: "9030", ThriftPort: "9020"}, Database: "dest_db"}
	if syncType == TableSync {
		src.Table, src.TableId = srcTable, resyncSrcTableId
		dest.Table, dest.TableId = destTable, resyncDestTableId
	}
	j := &Job{
		Name:     resyncJobName,
		Id:       "resync-state-job-id",
		SyncType: syncType,
		Src:      src,
		Dest:     dest,
		State:    JobRunning,
		Extra:    JobExtra{InterruptCh: make(chan any, 1)},
		factory:  f.factory,
		db:       f.db,
		stop:     make(chan struct{}),
	}
	j.ISrc = f.factory.NewSpecer(&j.Src)
	j.IDest = f.factory.NewSpecer(&j.Dest)
	j.srcMeta = f.factory.NewMeta(&j.Src)
	j.destMeta = f.factory.NewMeta(&j.Dest)
	j.jobFactory = NewJobFactory()
	j.ctx, j.cancel = context.WithCancel(context.Background())
	f.t.Cleanup(j.cancel)

	j.progress = NewJobProgress(j.Name, j.SyncType, f.db)
	j.progress.isDeleted = &j.isDeleted
	if syncType == TableSync {
		j.progress.SyncState = TableIncrementalSync
	} else {
		j.progress.SyncState = DBIncrementalSync
		j.progress.TableMapping = map[int64]int64{resyncSrcTableId: resyncDestTableId}
	}
	j.progress.SubSyncState = Done
	j.progress.CommitSeq = resyncCommitSeq
	j.progress.PrevCommitSeq = resyncCommitSeq
	j.progress.CcrResync = resync
	j.progress.Persist()
	return j
}

// restart builds a fresh Job and JobProgress from what is stored, the way Run does after a syncer restart.
func (f *resyncFixture) restart(j *Job) *Job {
	f.t.Helper()
	jobJson, err := json.Marshal(j)
	if err != nil {
		f.t.Fatalf("marshal job: %v", err)
	}
	restarted, err := NewJobFromJson(string(jobJson), f.db, f.factory)
	if err != nil {
		f.t.Fatalf("NewJobFromJson: %v", err)
	}
	if err := restarted.recoverJobProgress(); err != nil {
		f.t.Fatalf("recoverJobProgress: %v", err)
	}
	restarted.ctx, restarted.cancel = context.WithCancel(context.Background())
	f.t.Cleanup(restarted.cancel)
	return restarted
}

// stored is the progress a restarted syncer reads back, and whether the raw json has the ccr_resync key.
func (f *resyncFixture) stored() (*JobProgress, bool) {
	f.t.Helper()
	raw, err := f.db.GetProgress(resyncJobName)
	if err != nil {
		f.t.Fatalf("get progress: %v", err)
	}
	progress, err := NewJobProgressFromJson(resyncJobName, f.db)
	if err != nil {
		f.t.Fatalf("NewJobProgressFromJson: %v", err)
	}
	return progress, strings.Contains(raw, `"ccr_resync"`)
}

func withFlag(t *testing.T, flag *bool, value bool) {
	t.Helper()
	old := *flag
	*flag = value
	t.Cleanup(func() { *flag = old })
}

func withPipelineCommit(t *testing.T, value bool) {
	t.Helper()
	old := featurePipelineCommit
	featurePipelineCommit = value
	t.Cleanup(func() { featurePipelineCommit = old })
}

func partialResync(destPartitionIds ...int64) *ccrResync {
	return &ccrResync{TableId: resyncSrcTableId, DestPartitionIds: destPartitionIds}
}

func expectState(t *testing.T, what string, p *JobProgress, syncState SyncState, sub SubSyncState) {
	t.Helper()
	if p.SyncState != syncState || p.SubSyncState != sub {
		t.Fatalf("%s: state %s/%s, want %s/%s", what, p.SyncState, p.SubSyncState, syncState, sub)
	}
}

func expectPartialSyncData(t *testing.T, what string, p *JobProgress, table string, partitions []string) {
	t.Helper()
	want := &JobPartialSyncData{TableId: resyncSrcTableId, Table: table, Partitions: partitions}
	if !reflect.DeepEqual(p.PartialSyncData, want) {
		t.Fatalf("%s: partial sync data %+v, want %+v", what, p.PartialSyncData, want)
	}
}

func expectResync(t *testing.T, what string, got, want *ccrResync) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: ccr resync %+v, want %+v", what, got, want)
	}
}

// ---------------------------------------------------------------- 1. partial resync

func TestStartCcrResyncPartialTableSync(t *testing.T) {
	f := newResyncFixture(t)
	f.destMeta.partitionNames = map[int64]string{3001: "p2", 3002: "p1", 3003: "p2"}
	j := f.newJob(TableSync, "tbl", "tbl", partialResync(3001, 3002, 3003))

	if err := j.startCcrResync(); err != nil {
		t.Fatalf("startCcrResync: %v", err)
	}

	// the partition names are of the target table (the dest table id), sorted and without duplicates
	want := []string{"dest.GetPartitionName(2001,3001)", "dest.GetPartitionName(2001,3002)", "dest.GetPartitionName(2001,3003)"}
	if got := f.env.eventsWithPrefix("dest.GetPartitionName"); !reflect.DeepEqual(got, want) {
		t.Fatalf("partition name lookups %v, want %v", got, want)
	}
	for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": func() *JobProgress { p, _ := f.stored(); return p }()} {
		expectState(t, what, p, TablePartialSync, BeginCreateSnapshot)
		expectPartialSyncData(t, what, p, "tbl", []string{"p1", "p2"})
		expectResync(t, what, p.CcrResync, nil)
		if p.TableAliases != nil {
			t.Fatalf("%s: a partition resync must not replace the table, aliases %v", what, p.TableAliases)
		}
		if p.CommitSeq != resyncCommitSeq {
			t.Fatalf("%s: commit seq %d, want %d", what, p.CommitSeq, resyncCommitSeq)
		}
	}
	if _, hasKey := f.stored(); hasKey {
		t.Fatalf("the stored progress still has ccr_resync")
	}
}

func TestStartCcrResyncPartialDBSync(t *testing.T) {
	f := newResyncFixture(t)
	f.destMeta.tableNames = map[int64]string{resyncDestTableId: "dest_tbl"}
	f.destMeta.partitionNames = map[int64]string{3001: "p_b", 3002: "p_a"}
	j := f.newJob(DBSync, "", "", partialResync(3001, 3002))

	if err := j.startCcrResync(); err != nil {
		t.Fatalf("startCcrResync: %v", err)
	}

	if got := f.env.eventsWithPrefix("dest.GetPartitionName"); !reflect.DeepEqual(got,
		[]string{"dest.GetPartitionName(2001,3001)", "dest.GetPartitionName(2001,3002)"}) {
		t.Fatalf("the partition names must be looked up in the target table %d: %v", resyncDestTableId, got)
	}
	stored, hasKey := f.stored()
	for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
		expectState(t, what, p, DBPartialSync, BeginCreateSnapshot)
		// the table name is the target one
		expectPartialSyncData(t, what, p, "dest_tbl", []string{"p_a", "p_b"})
		expectResync(t, what, p.CcrResync, nil)
	}
	if hasKey {
		t.Fatalf("the stored progress still has ccr_resync")
	}
}

// ---------------------------------------------------------------- 2. restart recovery

func TestCcrResyncStartedAfterRestartClassic(t *testing.T) {
	withPipelineCommit(t, false)
	f := newResyncFixture(t)
	f.destMeta.tableNames = map[int64]string{resyncDestTableId: "dest_tbl"}
	f.destMeta.partitionNames = map[int64]string{3001: "p1"}
	before := f.newJob(DBSync, "", "", partialResync(3001))

	j := f.restart(before)
	expectResync(t, "recovered", j.progress.CcrResync, partialResync(3001))

	if err := j.sync(); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if got := f.env.eventsWithPrefix("rpc.GetBinlog"); len(got) != 0 {
		t.Fatalf("the tick must start the resync instead of getting binlogs: %v", got)
	}
	stored, hasKey := f.stored()
	for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
		expectState(t, what, p, DBPartialSync, BeginCreateSnapshot)
		expectPartialSyncData(t, what, p, "dest_tbl", []string{"p1"})
		expectResync(t, what, p.CcrResync, nil)
	}
	if hasKey {
		t.Fatalf("the stored progress still has ccr_resync")
	}
}

// The commit of the pipeline persists the resync with RollbackPipeline; after a restart the tick rolls back the
// later txns, goes to Done and starts the resync.
func TestCcrResyncStartedAfterRestartPipeline(t *testing.T) {
	withPipelineCommit(t, true)
	f := newResyncFixture(t)
	f.destMeta.tableNames = map[int64]string{resyncDestTableId: "dest_tbl"}
	f.destMeta.partitionNames = map[int64]string{3001: "p1"}
	before := f.newJob(DBSync, "", "", partialResync(3001))
	later := &TxnContext{CommitSeq: resyncCommitSeq + 1, TxnId: 502, DestTableIds: []int64{resyncDestTableId}}
	before.progress.NextSubCheckpoint(RollbackPipeline, &PipelineInMemoryData{RunningTxnList: []*TxnContext{later}})

	j := f.restart(before)
	expectState(t, "recovered", j.progress, DBIncrementalSync, RollbackPipeline)
	expectResync(t, "recovered", j.progress.CcrResync, partialResync(3001))

	if err := j.sync(); err != nil {
		t.Fatalf("tick: %v", err)
	}

	rollback := f.env.indexOf("rpc.RollbackTransaction(dest,502)")
	lookup := f.env.indexOf("dest.GetPartitionName")
	if rollback == -1 || lookup == -1 || rollback > lookup {
		t.Fatalf("the later txn must be rolled back before the resync starts, events %v", f.env.events)
	}
	if got := f.env.eventsWithPrefix("rpc.GetBinlog"); len(got) != 0 {
		t.Fatalf("the tick must start the resync instead of getting binlogs: %v", got)
	}
	stored, hasKey := f.stored()
	for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
		expectState(t, what, p, DBPartialSync, BeginCreateSnapshot)
		expectPartialSyncData(t, what, p, "dest_tbl", []string{"p1"})
		expectResync(t, what, p.CcrResync, nil)
	}
	if hasKey {
		t.Fatalf("the stored progress still has ccr_resync")
	}
	if j.pipelineCtx != nil {
		t.Fatalf("the pipeline must be reset when the resync starts")
	}
}

// ---------------------------------------------------------------- 3. transient lookup error

func TestStartCcrResyncTransientErrorKeepsPending(t *testing.T) {
	cases := []struct {
		name   string
		inject func(f *resyncFixture, err error)
		clear  func(f *resyncFixture)
	}{
		{
			name:   "table name",
			inject: func(f *resyncFixture, err error) { f.destMeta.tableNameErr = err },
			clear:  func(f *resyncFixture) { f.destMeta.tableNameErr = nil },
		},
		{
			name:   "partition name",
			inject: func(f *resyncFixture, err error) { f.destMeta.partitionErrs[3002] = err },
			clear:  func(f *resyncFixture) { delete(f.destMeta.partitionErrs, 3002) },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withPipelineCommit(t, false)
			f := newResyncFixture(t)
			f.destMeta.tableNames = map[int64]string{resyncDestTableId: "dest_tbl"}
			f.destMeta.partitionNames = map[int64]string{3001: "p1", 3002: "p2"}
			j := f.newJob(DBSync, "", "", partialResync(3001, 3002))
			fault := xerror.Errorf(xerror.Normal, "fake: the target FE is busy")
			c.inject(f, fault)

			err := j.sync()
			if !errors.Is(err, fault) {
				t.Fatalf("tick: %v, want the lookup error", err)
			}
			if panicErr := j.handleError(j.Name, err); panicErr != nil {
				t.Fatalf("handleError: %v", panicErr)
			}
			stored, hasKey := f.stored()
			for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
				expectState(t, what, p, DBIncrementalSync, Done)
				expectResync(t, what, p.CcrResync, partialResync(3001, 3002))
				if p.PartialSyncData != nil {
					t.Fatalf("%s: partial sync data %+v after a failed start", what, p.PartialSyncData)
				}
			}
			if !hasKey {
				t.Fatalf("the stored progress lost ccr_resync")
			}
			if got := f.env.eventsWithPrefix("rpc.GetBinlog"); len(got) != 0 {
				t.Fatalf("a pending resync must block the binlogs: %v", got)
			}

			c.clear(f)
			if err := j.sync(); err != nil {
				t.Fatalf("tick after the fault is removed: %v", err)
			}
			stored, hasKey = f.stored()
			for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
				expectState(t, what, p, DBPartialSync, BeginCreateSnapshot)
				expectPartialSyncData(t, what, p, "dest_tbl", []string{"p1", "p2"})
				expectResync(t, what, p.CcrResync, nil)
			}
			if hasKey {
				t.Fatalf("the stored progress still has ccr_resync")
			}
		})
	}
}

// ---------------------------------------------------------------- 4. source table dropped

func TestStartCcrResyncSourceTableDropped(t *testing.T) {
	withPipelineCommit(t, false)
	f := newResyncFixture(t)
	f.thrift.droppedTables = map[int64]struct{}{resyncSrcTableId: {}}
	j := f.newJob(DBSync, "", "", partialResync(3001))
	j.progress.TableMapping = nil // dropped by the drop table binlog after the commit
	j.progress.Persist()

	if err := j.sync(); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if got := f.env.eventsWithPrefix("dest."); len(got) != 0 {
		t.Fatalf("no target name lookup for a dropped table: %v", got)
	}
	if got := f.env.eventsWithPrefix("rpc.GetBinlog"); len(got) != 1 {
		t.Fatalf("the tick must go on with the binlogs after the skipped resync: %v", f.env.events)
	}
	stored, hasKey := f.stored()
	for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
		expectState(t, what, p, DBIncrementalSync, Done)
		expectResync(t, what, p.CcrResync, nil)
		if p.CommitSeq != resyncCommitSeq || p.PrevCommitSeq != resyncCommitSeq {
			t.Fatalf("%s: commit seq %d/%d moved", what, p.PrevCommitSeq, p.CommitSeq)
		}
	}
	if hasKey {
		t.Fatalf("the stored progress still has ccr_resync")
	}
}

// ---------------------------------------------------------------- 5. target partition gone

func TestStartCcrResyncTargetPartitionGoneReplacesTable(t *testing.T) {
	f := newResyncFixture(t)
	f.destMeta.tableNames = map[int64]string{resyncDestTableId: "dest_tbl"}
	f.destMeta.partitionNames = map[int64]string{3001: "p1"}
	f.destMeta.partitionErrs[3002] = xerror.Errorf(xerror.Meta, "fake: partition 3002 not found")
	j := f.newJob(DBSync, "", "", partialResync(3001, 3002))

	if err := j.startCcrResync(); err != nil {
		t.Fatalf("startCcrResync: %v", err)
	}

	stored, hasKey := f.stored()
	for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
		expectState(t, what, p, DBPartialSync, BeginCreateSnapshot)
		expectPartialSyncData(t, what, p, "dest_tbl", nil)
		expectResync(t, what, p.CcrResync, nil)
		alias, ok := p.TableAliases["dest_tbl"]
		if len(p.TableAliases) != 1 || !ok || !strings.HasPrefix(alias, "__ccr_dest_tbl_") {
			t.Fatalf("%s: the whole table must be replaced, aliases %v", what, p.TableAliases)
		}
	}
	if hasKey {
		t.Fatalf("the stored progress still has ccr_resync")
	}
}

// ---------------------------------------------------------------- 6. full sync resync

func TestStartCcrResyncFullSync(t *testing.T) {
	for _, syncType := range []SyncType{TableSync, DBSync} {
		t.Run(syncType.String(), func(t *testing.T) {
			f := newResyncFixture(t)
			resync := &ccrResync{FullSyncInfo: "the not continuous target partitions are of more than one table"}
			j := f.newJob(syncType, "tbl", "tbl", resync)
			// a partial snapshot a later ingest asked for, covered by the full sync
			j.Extra.PartialSnapshotParams = &PartialSnapshotParams{TableId: 7, TableName: "other"}
			syncId := j.progress.SyncId

			if err := j.startCcrResync(); err != nil {
				t.Fatalf("startCcrResync: %v", err)
			}

			want := DBFullSync
			if syncType == TableSync {
				want = TableFullSync
			}
			if j.Extra.PartialSnapshotParams != nil {
				t.Fatalf("the partial snapshot params %+v survive the full sync", j.Extra.PartialSnapshotParams)
			}
			if got := f.env.eventsWithPrefix("dest."); len(got) != 0 {
				t.Fatalf("no name lookup for a full sync: %v", got)
			}
			stored, hasKey := f.stored()
			for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
				expectState(t, what, p, want, BeginCreateSnapshot)
				expectResync(t, what, p.CcrResync, nil)
				if p.FullSyncInfo.Info != resync.FullSyncInfo || p.FullSyncInfo.CommitSeq != resyncCommitSeq {
					t.Fatalf("%s: full sync info %+v", what, p.FullSyncInfo)
				}
				if p.CommitSeq != resyncCommitSeq || p.SyncId != syncId+1 {
					t.Fatalf("%s: commit seq %d sync id %d", what, p.CommitSeq, p.SyncId)
				}
			}
			if hasKey {
				t.Fatalf("the stored progress still has ccr_resync")
			}
		})
	}
}

// ---------------------------------------------------------------- 7. a full sync for another reason

func TestNewSnapshotDropsPendingCcrResync(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		f := newResyncFixture(t)
		j := f.newJob(DBSync, "", "", partialResync(3001))

		if err := j.NewSnapshot(j.progress.CommitSeq, "another reason"); err != nil {
			t.Fatalf("NewSnapshot: %v", err)
		}
		stored, hasKey := f.stored()
		for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
			expectState(t, what, p, DBFullSync, BeginCreateSnapshot)
			expectResync(t, what, p.CcrResync, nil)
		}
		if hasKey {
			t.Fatalf("the stored progress still has ccr_resync")
		}
	})

	// a meta error of the resync start goes through handleError, which makes the full sync
	t.Run("meta error of the start", func(t *testing.T) {
		withPipelineCommit(t, false)
		f := newResyncFixture(t)
		f.destMeta.tableNameErr = xerror.Errorf(xerror.Meta, "fake: table %d not found", resyncDestTableId)
		j := f.newJob(DBSync, "", "", partialResync(3001))

		err := j.sync()
		if !xerror.IsCategory(err, xerror.Meta) {
			t.Fatalf("tick: %v, want a meta error", err)
		}
		if panicErr := j.handleError(j.Name, err); panicErr != nil {
			t.Fatalf("handleError: %v", panicErr)
		}
		stored, hasKey := f.stored()
		for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
			expectState(t, what, p, DBFullSync, BeginCreateSnapshot)
			expectResync(t, what, p.CcrResync, nil)
		}
		if hasKey {
			t.Fatalf("the stored progress still has ccr_resync")
		}
	})
}

// ---------------------------------------------------------------- 8. the skip the user required first

func TestUserFullSyncSkipBeforeCcrResync(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		t.Run(fmt.Sprintf("pipeline=%t", pipeline), func(t *testing.T) {
			withPipelineCommit(t, pipeline)
			f := newResyncFixture(t)
			f.destMeta.tableNames = map[int64]string{resyncDestTableId: "dest_tbl"}
			f.destMeta.partitionNames = map[int64]string{3001: "p1"}
			j := f.newJob(DBSync, "", "", partialResync(3001))
			j.Extra.SkipBinlog = true
			j.Extra.SkipBy = SkipByFullSync

			if err := j.sync(); err != nil {
				t.Fatalf("tick: %v", err)
			}

			if got := f.env.eventsWithPrefix("dest."); len(got) != 0 {
				t.Fatalf("the resync must not be attempted before the user's full sync: %v", got)
			}
			stored, hasKey := f.stored()
			for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
				expectState(t, what, p, DBFullSync, BeginCreateSnapshot)
				expectResync(t, what, p.CcrResync, nil)
				if !strings.Contains(p.FullSyncInfo.Info, "the user required skipping the binlog") {
					t.Fatalf("%s: full sync info %q, want the user's skip", what, p.FullSyncInfo.Info)
				}
			}
			if hasKey {
				t.Fatalf("the stored progress still has ccr_resync")
			}
		})
	}
}

// ---------------------------------------------------------------- 9. table sync with alias

func TestStartCcrResyncTableSyncWithAlias(t *testing.T) {
	f := newResyncFixture(t)
	f.destMeta.partitionNames = map[int64]string{3001: "p1"}
	j := f.newJob(TableSync, "src_tbl", "dest_tbl", partialResync(3001))
	if !j.IsTableSyncWithAlias() {
		t.Fatalf("the job must be a table sync with alias")
	}

	if err := j.startCcrResync(); err != nil {
		t.Fatalf("startCcrResync: %v", err)
	}

	stored, _ := f.stored()
	for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
		expectState(t, what, p, TablePartialSync, BeginCreateSnapshot)
		expectPartialSyncData(t, what, p, "src_tbl", []string{"p1"})
		expectResync(t, what, p.CcrResync, nil)
	}
}

// ---------------------------------------------------------------- 10. the wait of PrevTxnId fails

func TestStartCcrResyncPrevTxnWaitFails(t *testing.T) {
	withPipelineCommit(t, false)
	f := newResyncFixture(t)
	f.destMeta.tableNames = map[int64]string{resyncDestTableId: "dest_tbl"}
	f.destMeta.partitionNames = map[int64]string{3001: "p1"}
	j := f.newJob(DBSync, "", "", partialResync(3001))
	j.progress.PrevTxnId = 77 // the committed txn, not known visible yet
	j.progress.Persist()
	j.cancel() // the wait ends with the error of the job context

	err := j.sync()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("tick: %v, want the error of the wait", err)
	}
	if got := f.env.eventsWithPrefix("dest."); len(got) != 0 {
		t.Fatalf("no name lookup before the txn is visible: %v", got)
	}
	stored, hasKey := f.stored()
	for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
		expectState(t, what, p, DBIncrementalSync, Done)
		expectResync(t, what, p.CcrResync, partialResync(3001))
		if p.PrevTxnId != 77 {
			t.Fatalf("%s: prev txn id %d, want 77", what, p.PrevTxnId)
		}
	}
	if !hasKey {
		t.Fatalf("the stored progress lost ccr_resync")
	}
}

// ---------------------------------------------------------------- 11. the commit plans the resync (flag on / off)

func notContinuousCommitResult(partitionIds ...int64) *festruct.TCommitTxnResult_ {
	resp := festruct.NewTCommitTxnResult_()
	resp.Status = &tstatus.TStatus{StatusCode: tstatus.TStatusCode_OK}
	resp.CcrNotContinuousPartitionIds = partitionIds
	return resp
}

// The classic commit: a restart left the upsert in CommitTransaction, the tick recovers it (handleUpsert) and commits.
func TestHandleUpsertCommitPlansCcrResync(t *testing.T) {
	for _, flagOn := range []bool{true, false} {
		t.Run(fmt.Sprintf("flag=%t", flagOn), func(t *testing.T) {
			withPipelineCommit(t, false)
			withFlag(t, resyncNotContinuousPartitions, flagOn)
			f := newResyncFixture(t)
			f.fe.commitResult = notContinuousCommitResult(3001)
			j := f.newJob(DBSync, "", "", nil)

			commitSeq := resyncCommitSeq + 1
			sources := []*festruct.TCcrPartitionSource{ccrSource(3001, 4001)}
			persisted := map[string]any{
				"commit_seq":     commitSeq,
				"txn_id":         int64(601),
				"dest_table_ids": []int64{resyncDestTableId},
				"table_records":  []*record.TableRecord{ccrTableRecord(resyncSrcTableId, 4001)},
				"commit_infos":   []any{},
				"ccr_sources":    sources,
			}
			j.progress.StartHandle(commitSeq)
			j.progress.NextSubCheckpoint(CommitTransaction, persisted)
			j = f.restart(j)
			j.cancel() // a resync start would block on the wait of the committed txn

			if err := j.sync(); err != nil {
				t.Fatalf("tick: %v", err)
			}

			if len(f.fe.commitSources) != 1 || !reflect.DeepEqual(f.fe.commitSources[0], sources) {
				t.Fatalf("the commit must carry the ccr sources, got %v", f.fe.commitSources)
			}
			var want *ccrResync
			if flagOn {
				want = partialResync(3001)
			}
			stored, hasKey := f.stored()
			for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
				expectState(t, what, p, DBIncrementalSync, Done)
				expectResync(t, what, p.CcrResync, want)
				if p.CommitSeq != commitSeq || p.PrevCommitSeq != commitSeq || p.PrevTxnId != 601 {
					t.Fatalf("%s: commit seq %d/%d prev txn %d", what, p.PrevCommitSeq, p.CommitSeq, p.PrevTxnId)
				}
			}
			if hasKey != flagOn {
				t.Fatalf("stored ccr_resync key %t, want %t", hasKey, flagOn)
			}
			if !flagOn {
				return
			}

			// the next tick starts it: it waits the committed txn first, and keeps it pending when the wait fails
			err := j.sync()
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("next tick: %v, want the error of the wait of txn 601", err)
			}
			if got := f.env.eventsWithPrefix("rpc.GetBinlog"); len(got) != 0 {
				t.Fatalf("a pending resync must block the binlogs: %v", got)
			}
			stored, _ = f.stored()
			expectResync(t, "stored after the next tick", stored.CcrResync, want)
		})
	}
}

// The pipeline commit: the txn of the not continuous partitions is committed, the resync is persisted with
// RollbackPipeline, the later running txn is rolled back, and the Done state starts the resync.
func TestPipelineCommitPlansCcrResync(t *testing.T) {
	for _, flagOn := range []bool{true, false} {
		t.Run(fmt.Sprintf("flag=%t", flagOn), func(t *testing.T) {
			withPipelineCommit(t, true)
			withFlag(t, resyncNotContinuousPartitions, flagOn)
			f := newResyncFixture(t)
			f.fe.commitResult = notContinuousCommitResult(3001)
			stopErr := errors.New("fake: stop the pipeline")
			f.fe.getBinlog = func() (*festruct.TGetBinlogResult_, error) { return nil, stopErr }
			j := f.newJob(DBSync, "", "", nil)
			j.cancel() // a resync start would block on the wait of the committed txn

			committed := &TxnContext{CommitSeq: resyncCommitSeq + 1, TxnId: 501, DestTableIds: []int64{resyncDestTableId},
				CcrSources:   []*festruct.TCcrPartitionSource{ccrSource(3001, 4001)},
				TableRecords: []*record.TableRecord{ccrTableRecord(resyncSrcTableId, 4001)}}
			running := []*TxnContext{committed}
			if flagOn {
				running = append(running, &TxnContext{CommitSeq: resyncCommitSeq + 2, TxnId: 502,
					DestTableIds: []int64{resyncDestTableId}})
			}
			// as the CommitPipeline state leaves it: the data in memory, checkpointed in CommitPipelineTransaction
			data := &PipelineInMemoryData{RunningTxnList: running, CommitableTxn: committed}
			j.initializePipelineContext()
			j.pipelineCtx.NextCommitSeq = running[len(running)-1].CommitSeq // the binlogs of the launched txns are fetched
			j.progress.NextSubVolatile(CommitPipeline, data)
			j.progress.NextSubCheckpoint(CommitPipelineTransaction, data)

			err := j.sync()

			if got := f.env.eventsWithPrefix("rpc.CommitTransaction"); !reflect.DeepEqual(got,
				[]string{"rpc.CommitTransaction(dest,501)"}) {
				t.Fatalf("commits %v", got)
			}
			stored, hasKey := f.stored()
			if !flagOn {
				// the pipeline goes on fetching binlogs, the fake stops it
				if !errors.Is(err, stopErr) {
					t.Fatalf("tick: %v, want the fake stop", err)
				}
				for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
					expectResync(t, what, p.CcrResync, nil)
				}
				if hasKey {
					t.Fatalf("the stored progress has ccr_resync with the flag off")
				}
				for _, progress := range f.db.history() {
					if strings.Contains(progress, `"ccr_resync"`) {
						t.Fatalf("a progress with ccr_resync was persisted with the flag off: %s", progress)
					}
				}
				return
			}

			if !errors.Is(err, context.Canceled) {
				t.Fatalf("tick: %v, want the error of the wait of txn 501 by the resync start", err)
			}
			// the resync is persisted in the same write as the commit seq and RollbackPipeline
			persistedWithRollback := false
			for _, progress := range f.db.history() {
				var p JobProgress
				if err := json.Unmarshal([]byte(progress), &p); err != nil {
					t.Fatalf("unmarshal persisted progress: %v", err)
				}
				if p.SubSyncState == RollbackPipeline {
					if p.CommitSeq != committed.CommitSeq || !reflect.DeepEqual(p.CcrResync, partialResync(3001)) {
						t.Fatalf("RollbackPipeline persisted without the commit and the resync: %s", progress)
					}
					persistedWithRollback = true
				}
			}
			if !persistedWithRollback {
				t.Fatalf("RollbackPipeline was never persisted: %v", f.db.history())
			}
			if got := f.env.eventsWithPrefix("rpc.RollbackTransaction"); !reflect.DeepEqual(got,
				[]string{"rpc.RollbackTransaction(dest,502)"}) {
				t.Fatalf("the later txn must be rolled back, got %v", got)
			}
			if got := f.env.eventsWithPrefix("rpc.GetBinlog"); len(got) != 0 {
				t.Fatalf("the pipeline must not go on after the not continuous commit: %v", got)
			}
			for what, p := range map[string]*JobProgress{"memory": j.progress, "stored": stored} {
				expectState(t, what, p, DBIncrementalSync, Done)
				expectResync(t, what, p.CcrResync, partialResync(3001))
				if p.CommitSeq != committed.CommitSeq || p.PrevTxnId != 501 {
					t.Fatalf("%s: commit seq %d prev txn %d", what, p.CommitSeq, p.PrevTxnId)
				}
			}
			if !hasKey {
				t.Fatalf("the stored progress lost ccr_resync")
			}
			if j.pipelineCtx != nil {
				t.Fatalf("the pipeline must be reset")
			}
		})
	}
}
