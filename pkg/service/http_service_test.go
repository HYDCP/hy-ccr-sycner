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
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
)

func TestInvalidateBackendsCacheHandlerRequestBody(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantSuccess bool
		wantCount   int
	}{
		{
			name:        "empty body invalidates all jobs",
			body:        "",
			wantSuccess: true,
			wantCount:   0,
		},
		{
			name:        "empty object invalidates all jobs",
			body:        `{}`,
			wantSuccess: true,
			wantCount:   0,
		},
		{
			name:        "valid named request reaches job manager",
			body:        `{"name":"missing"}`,
			wantSuccess: false,
			wantCount:   0,
		},
		{
			name:        "truncated json is rejected",
			body:        `{"name":"missing"`,
			wantSuccess: false,
			wantCount:   0,
		},
		{
			name:        "json null is rejected",
			body:        `null`,
			wantSuccess: false,
			wantCount:   0,
		},
		{
			name:        "trailing invalid data is rejected",
			body:        `{} trailing`,
			wantSuccess: false,
			wantCount:   0,
		},
		{
			name:        "second json value is rejected",
			body:        `{} {}`,
			wantSuccess: false,
			wantCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &HttpService{
				jobManager: ccr.NewJobManager(nil, nil, "test-syncer"),
			}
			request := httptest.NewRequest("POST", "/invalidate_backends_cache", strings.NewReader(tt.body))
			response := httptest.NewRecorder()

			service.invalidateBackendsCacheHandler(response, request)

			var got struct {
				Success          bool   `json:"success"`
				ErrorMsg         string `json:"error_msg"`
				InvalidatedCount int    `json:"invalidated_count"`
			}
			if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if got.Success != tt.wantSuccess {
				t.Fatalf("success = %v, want %v; error = %q", got.Success, tt.wantSuccess, got.ErrorMsg)
			}
			if got.InvalidatedCount != tt.wantCount {
				t.Fatalf("invalidated_count = %d, want %d", got.InvalidatedCount, tt.wantCount)
			}
			if !tt.wantSuccess && got.ErrorMsg == "" {
				t.Fatal("expected a non-empty error message")
			}
		})
	}
}
