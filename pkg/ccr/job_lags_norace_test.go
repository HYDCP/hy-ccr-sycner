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

//go:build !race

// Job.Run calls gls.GoID, whose pointer arithmetic aborts under the race
// detector's checkptr instrumentation, so this test only runs without -race.

package ccr

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/stretchr/testify/require"
)

// viewTestDB serves the job info for Recover and fails IsProgressExist, so the
// recovered job goroutine exits right away while the job stays registered.
type viewTestDB struct{ storage.DB }

func (viewTestDB) GetJobInfo(name string) (string, error) {
	data, err := json.Marshal(&Job{Name: name, State: JobRunning})
	return string(data), err
}

func (viewTestDB) IsProgressExist(string) (bool, error) { return false, errors.New("meta db down") }

func viewNames(jm *JobManager) []string {
	names := []string{}
	for _, job := range jm.JobsView() {
		names = append(names, job.Name)
	}
	return names
}

func TestJobsViewFollowsRecoverAndDelete(t *testing.T) {
	factory := NewFactory(nil, NewMetaFactory(), base.NewSpecerFactory(), DefaultThriftMetaFactory)
	jm := NewJobManager(viewTestDB{}, factory, "test-syncer")

	require.NoError(t, jm.Recover([]string{"job_b", "job_a"}))
	jm.wg.Wait()
	require.Equal(t, []string{"job_a", "job_b"}, viewNames(jm))

	// A deleted job leaves the view when its goroutine exits; a nil meta db
	// makes this Run panic, which runJob recovers.
	jm.lock.Lock()
	deleted := jm.jobs["job_a"]
	deleted.db = nil
	deleted.isDeleted.Store(true)
	jm.lock.Unlock()
	jm.runJob(deleted)
	jm.wg.Wait()
	require.Equal(t, []string{"job_b"}, viewNames(jm))
}
