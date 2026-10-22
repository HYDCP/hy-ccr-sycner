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
package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestCreateCcrLogOmitsPasswords(t *testing.T) {
	const srcPassword = "src-s3cret-Passw0rd"
	const destPassword = "dest-s3cret-Passw0rd"

	hook := logtest.NewGlobal()
	defer hook.Reset()

	factory := ccr.NewFactory(nil, ccr.NewMetaFactory(), base.NewSpecerFactory(), nil)
	service := NewHttpServer("127.0.0.1", 9190, nil, ccr.NewJobManager(nil, factory, "test-syncer"))
	service.RegisterHandlers()

	// replication_num 0 is rejected right after the request is logged, before
	// the job touches the meta db or the clusters.
	body := `{
		"name": "job1",
		"src": {"host": "10.0.0.1", "port": "9030", "thrift_port": "9020",
			"user": "src_user", "password": "` + srcPassword + `", "database": "db1"},
		"dest": {"host": "10.0.0.2", "port": "9030", "thrift_port": "9020",
			"user": "dest_user", "password": "` + destPassword + `", "database": "db1"},
		"replication_num": 0
	}`
	response := httptest.NewRecorder()
	service.mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/create_ccr", strings.NewReader(body)))
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), "invalid replication_num")

	var requestLogged bool
	for _, entry := range hook.AllEntries() {
		require.NotContains(t, entry.Message, srcPassword, "log: %s", entry.Message)
		require.NotContains(t, entry.Message, destPassword, "log: %s", entry.Message)
		if strings.HasPrefix(entry.Message, "create ccr name: job1") {
			requestLogged = true
			require.Contains(t, entry.Message, "host: 10.0.0.1", "log: %s", entry.Message)
			require.Contains(t, entry.Message, "host: 10.0.0.2", "log: %s", entry.Message)
		}
	}
	require.True(t, requestLogged, "the create ccr request is not logged")
}
