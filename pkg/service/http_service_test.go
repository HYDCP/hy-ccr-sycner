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
// under the License.
package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterHandlersKeepsNodeInfoWithoutMigrationEndpoints(t *testing.T) {
	jobManager := ccr.NewJobManager(nil, nil, "test-syncer")
	service := NewHttpServer("127.0.0.1", 9190, nil, jobManager)
	service.RegisterHandlers()

	t.Run("node info remains available", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/node_info", nil)
		response := httptest.NewRecorder()

		service.mux.ServeHTTP(response, request)

		assert.Equal(t, http.StatusOK, response.Code)
		var result map[string]interface{}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		assert.Equal(t, true, result["success"])
		assert.Equal(t, "127.0.0.1", result["host"])
		assert.Equal(t, float64(9190), result["port"])
	})

	for _, path := range []string{"/migrate", "/notify_update"} {
		t.Run(path+" is not registered", func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, path, nil)
			response := httptest.NewRecorder()

			service.mux.ServeHTTP(response, request)

			assert.Equal(t, http.StatusNotFound, response.Code)
		})
	}
}
