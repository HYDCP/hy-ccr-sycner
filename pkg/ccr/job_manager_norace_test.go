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
	"errors"
	"strings"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/stretchr/testify/require"
)

// runTestDB fails IsProgressExist, so Job.Run returns an error before syncing.
type runTestDB struct{ storage.DB }

func (runTestDB) IsProgressExist(string) (bool, error) { return false, errors.New("meta db down") }

func TestRunJobRecordsPanicAndRunError(t *testing.T) {
	jm := NewJobManager(nil, nil, "test-syncer")

	// A nil meta db makes Job.Run panic; runJob recovers it.
	panicJob := &Job{Name: "panic_job"}
	jm.runJob(panicJob)
	jm.wg.Wait()
	status := panicJob.Status()
	require.True(t, status.Panicked)
	require.Equal(t, int64(1), status.ConsecutiveFailures)
	require.Equal(t, "unknown", status.LastErrorCategory)
	require.True(t, strings.HasPrefix(status.LastError, "panic: "), status.LastError)

	// A Run error ends the job goroutine for good too, so it counts as panicked.
	failedJob := &Job{Name: "failed_job", db: runTestDB{}}
	jm.runJob(failedJob)
	jm.wg.Wait()
	status = failedJob.Status()
	require.True(t, status.Panicked)
	require.Equal(t, int64(1), status.ConsecutiveFailures)
	require.Contains(t, status.LastError, "meta db down")
}
