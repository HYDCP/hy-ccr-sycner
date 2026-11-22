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
	"flag"
	"fmt"
	"sort"

	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	log "github.com/sirupsen/logrus"
)

// restore reuse: the target FE tells, in the result of a commit, the target partitions whose state token before the
// write was not the one the source had, so the increment did not continue them (the target partition is not the
// content the source wrote on). The transaction is committed already, so the partitions are synced again from the
// source: the partial snapshot is taken after the commit, and the restore keeps the partitions whose token matches.
// A commit whose result is not received (the rpc fails or times out, and the rollback then finds the txn committed),
// or is received and lost by a restart before it is persisted, tells nothing, and its partitions are not synced again.
//
// Off by default: a target partition whose token was never aligned with the source (the tracking of the tokens was
// just switched on, or the source wrote it in a transaction of sub transactions) is told as not continuous too, and
// is downloaded as a whole by the resync. Switch it on after a full sync has aligned the tokens.
var resyncNotContinuousPartitions = flag.Bool("resync_not_continuous_partitions", false,
	"sync again the partitions the target FE reports as not continuing their CCR source after a commit")

// ccrResync is what to sync again for the not continuous partitions of a commit: the target partitions of one source
// table, or the full sync (FullSyncInfo set) when the partitions are not of one source table or cannot be located.
// It is persisted in the progress with the commit and until it is started, the names are resolved when it is started.
type ccrResync struct {
	TableId          int64   `json:"table_id,omitempty"`
	DestPartitionIds []int64 `json:"dest_partition_ids,omitempty"`
	FullSyncInfo     string  `json:"full_sync_info,omitempty"`
}

// planCcrResync maps the not continuous target partitions back to the source table, by the CCR sources sent with the
// commit (target partition id -> source partition id) and the table records of the upsert (source partition id ->
// source table id).
func planCcrResync(notContinuous []int64, sources []*festruct.TCcrPartitionSource,
	tableRecords []*record.TableRecord) *ccrResync {
	srcPartitionOf := make(map[int64]int64, len(sources))
	for _, src := range sources {
		srcPartitionOf[src.GetTargetPartitionId()] = src.GetSrcPartitionId()
	}
	srcTableOf := make(map[int64]int64)
	for _, tableRecord := range tableRecords {
		for _, partitionRecord := range tableRecord.PartitionRecords {
			srcTableOf[partitionRecord.Id] = tableRecord.Id
		}
	}

	srcTableId := int64(-1)
	for _, targetId := range notContinuous {
		srcPartitionId, ok := srcPartitionOf[targetId]
		if !ok {
			return &ccrResync{FullSyncInfo: fmt.Sprintf(
				"the not continuous target partition %d is not one of the ccr partition sources", targetId)}
		}
		tableId, ok := srcTableOf[srcPartitionId]
		if !ok {
			return &ccrResync{FullSyncInfo: fmt.Sprintf(
				"the source partition %d of the not continuous target partition %d is not in the table records",
				srcPartitionId, targetId)}
		}
		if srcTableId != -1 && srcTableId != tableId {
			return &ccrResync{FullSyncInfo: fmt.Sprintf(
				"the not continuous target partitions %v are of more than one source table", notContinuous)}
		}
		srcTableId = tableId
	}
	return &ccrResync{TableId: srcTableId, DestPartitionIds: notContinuous}
}

// ccrResyncSnapshotParams names the partitions of a partial resync of the table. The names are those of the target,
// which has the names of the source as of the committed binlog: the source may have renamed the table or a partition
// since, and the partial sync handles a name the source no longer has. It returns the error of a name that cannot be
// got now, the resync is retried on the next tick.
func ccrResyncSnapshotParams(resync *ccrResync, table string,
	partitionName func(int64) (string, error)) (*PartialSnapshotParams, error) {
	names := make([]string, 0, len(resync.DestPartitionIds))
	seen := make(map[string]bool, len(resync.DestPartitionIds))
	for _, partitionId := range resync.DestPartitionIds {
		name, err := partitionName(partitionId)
		if xerror.IsCategory(err, xerror.Meta) {
			// the target partition is gone since the commit, sync the whole table
			log.Warnf("get the name of the target partition %d of table %s: %v, sync the whole table",
				partitionId, table, err)
			return &PartialSnapshotParams{TableId: resync.TableId, TableName: table, Replace: true}, nil
		} else if err != nil {
			return nil, err
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return &PartialSnapshotParams{TableId: resync.TableId, TableName: table, Partitions: names}, nil
}

// planCcrNotContinuousResync plans to sync again the not continuous partitions of a committed transaction, it
// returns nil if there are none (an older FE never tells) or the resync is switched off.
func (j *Job) planCcrNotContinuousResync(commitSeq, txnId int64, notContinuous []int64,
	sources []*festruct.TCcrPartitionSource, tableRecords []*record.TableRecord) *ccrResync {
	if len(notContinuous) == 0 {
		return nil
	}
	if !*resyncNotContinuousPartitions {
		log.Warnf("txn %d of commit seq %d did not continue the ccr sources of target partitions %v, "+
			"the resync is switched off", txnId, commitSeq, notContinuous)
		return nil
	}

	resync := planCcrResync(notContinuous, sources, tableRecords)
	if resync.FullSyncInfo == "" {
		log.Warnf("txn %d of commit seq %d did not continue the ccr sources of target partitions %v, "+
			"planned to sync them again, source table %d", txnId, commitSeq, notContinuous, resync.TableId)
	} else {
		log.Warnf("txn %d of commit seq %d did not continue the ccr sources of target partitions %v, "+
			"planned to full sync because %s", txnId, commitSeq, notContinuous, resync.FullSyncInfo)
	}
	return resync
}

// startCcrResync starts the resync persisted in the progress, from the commit seq of the committed transaction: the
// snapshot is taken after it, and the binlogs it contains are filtered as for any partial or full sync.
func (j *Job) startCcrResync() error {
	resync := j.progress.CcrResync
	if err := j.startCcrResyncInternal(resync); err != nil {
		log.Warnf("start the resync of the not continuous partitions %+v failed, retry on the next tick: %v",
			*resync, err)
		return err
	}
	return nil
}

func (j *Job) startCcrResyncInternal(resync *ccrResync) error {

	// the txn is only committed in the default commit mode, the restore of its partitions waits it visible
	if prevTxnId := j.progress.PrevTxnId; prevTxnId != -1 {
		dest := &j.Dest
		log.Infof("wait prev txn id: %d published", prevTxnId)
		if err := dest.WaitTransactionDoneWithContext(j.ctx, prevTxnId); err != nil {
			return err
		}
		j.progress.PrevTxnId = -1
	}

	if resync.FullSyncInfo != "" {
		log.Warnf("start the full sync of the not continuous partitions, because %s", resync.FullSyncInfo)
		// a partial snapshot the ingest of a later txn triggered is covered by the full sync
		j.Extra.PartialSnapshotParams = nil
		return j.NewSnapshot(j.progress.CommitSeq, resync.FullSyncInfo)
	}

	destTableId, err := j.GetDestTableIdBySrc(resync.TableId)
	if err != nil {
		return err
	}
	if destTableId == 0 {
		// the drop table binlog after the commit drops the target table
		log.Warnf("the source table %d of the not continuous partitions %v is dropped, skip the resync",
			resync.TableId, resync.DestPartitionIds)
		j.progress.CcrResync = nil
		j.progress.Persist()
		return nil
	}
	table, err := j.GetDestNameBySrcId(resync.TableId)
	if err != nil {
		return err
	}
	if j.IsTableSyncWithAlias() {
		table = j.Src.Table
	}
	partitionName := func(partitionId int64) (string, error) {
		return j.destMeta.GetPartitionName(destTableId, partitionId)
	}
	params, err := ccrResyncSnapshotParams(resync, table, partitionName)
	if err != nil {
		return err
	}
	j.progress.CcrResync = nil
	log.Warnf("start to sync table %s partitions %v again (replace: %t), they did not continue the ccr sources",
		params.TableName, params.Partitions, params.Replace)
	return j.NewPartialSnapshot(params.TableId, params.TableName, params.Partitions, params.Replace, params.IsView)
}
