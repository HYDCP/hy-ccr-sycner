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
	"strings"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/version"
	"github.com/stretchr/testify/require"
)

func TestReleaseNodeInfoWithoutMigration(t *testing.T) {
	service := NewHttpServer("127.0.0.1", 9190, nil, ccr.NewJobManager(nil, nil, "test-syncer"))
	service.RegisterHandlers()
	response := httptest.NewRecorder()
	service.mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/node_info", nil))
	require.Equal(t, http.StatusOK, response.Code)
	var node struct {
		Success       bool   `json:"success"`
		Version       string `json:"version"`
		Host          string `json:"host"`
		Port          int    `json:"port"`
		UptimeSeconds int64  `json:"uptime_seconds"`
		Config        struct {
			DbType string `json:"db_type"`
		} `json:"config"`
		Resources struct {
			GoroutineCount int `json:"goroutine_count"`
		} `json:"resources"`
		Tasks struct {
			Total   int `json:"total"`
			Running int `json:"running"`
			Paused  int `json:"paused"`
		} `json:"tasks"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&node))
	require.True(t, node.Success)
	require.Equal(t, version.GetVersion(), node.Version)
	require.Equal(t, "127.0.0.1", node.Host)
	require.Equal(t, 9190, node.Port)
	require.GreaterOrEqual(t, node.UptimeSeconds, int64(0))
	require.NotEmpty(t, node.Config.DbType)
	require.Positive(t, node.Resources.GoroutineCount)
	require.Zero(t, node.Tasks.Total)
	require.Zero(t, node.Tasks.Running)
	require.Zero(t, node.Tasks.Paused)

	response = httptest.NewRecorder()
	service.mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/invalidate_backends_cache", nil))
	require.Equal(t, http.StatusOK, response.Code)
	var invalidation struct {
		Success          bool `json:"success"`
		InvalidatedCount int  `json:"invalidated_count"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&invalidation))
	require.True(t, invalidation.Success)
	require.Zero(t, invalidation.InvalidatedCount)

	for _, path := range []string{"/migrate", "/notify_update"} {
		response = httptest.NewRecorder()
		service.mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
		require.Equal(t, http.StatusNotFound, response.Code, path)
	}
}

func TestInvalidateBackendsCacheHandlerRequestBody(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		body        string
		wantSuccess bool
		wantCount   int
		wantStatus  int
		wantAllow   string
	}{
		{
			name:        "empty body invalidates all jobs",
			method:      http.MethodPost,
			body:        "",
			wantSuccess: true,
			wantCount:   0,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "empty object invalidates all jobs",
			method:      http.MethodPost,
			body:        `{}`,
			wantSuccess: true,
			wantCount:   0,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "valid named request reaches job manager",
			method:      http.MethodPost,
			body:        `{"name":"missing"}`,
			wantSuccess: false,
			wantCount:   0,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "truncated json is rejected",
			method:      http.MethodPost,
			body:        `{"name":"missing"`,
			wantSuccess: false,
			wantCount:   0,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "json null is rejected",
			method:      http.MethodPost,
			body:        `null`,
			wantSuccess: false,
			wantCount:   0,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "trailing invalid data is rejected",
			method:      http.MethodPost,
			body:        `{} trailing`,
			wantSuccess: false,
			wantCount:   0,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "second json value is rejected",
			method:      http.MethodPost,
			body:        `{} {}`,
			wantSuccess: false,
			wantCount:   0,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "unknown field is rejected",
			method:      http.MethodPost,
			body:        `{"nmae":"missing"}`,
			wantSuccess: false,
			wantCount:   0,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "get is rejected before parsing",
			method:      http.MethodGet,
			body:        "",
			wantSuccess: false,
			wantCount:   0,
			wantStatus:  http.StatusMethodNotAllowed,
			wantAllow:   http.MethodPost,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &HttpService{
				jobManager: ccr.NewJobManager(nil, nil, "test-syncer"),
			}
			request := httptest.NewRequest(tt.method, "/invalidate_backends_cache", strings.NewReader(tt.body))
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
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, tt.wantStatus)
			}
			if allow := response.Header().Get("Allow"); allow != tt.wantAllow {
				t.Fatalf("Allow header = %q, want %q", allow, tt.wantAllow)
			}
			if !tt.wantSuccess && got.ErrorMsg == "" {
				t.Fatal("expected a non-empty error message")
			}
		})
	}
}

func TestJobLagsHandler(t *testing.T) {
	jobManager := ccr.NewJobManager(nil, nil, "test-syncer")
	service := NewHttpServer("127.0.0.1", 9190, nil, jobManager)
	service.RegisterHandlers()
	get := func(method string) (int, map[string]any) {
		response := httptest.NewRecorder()
		service.mux.ServeHTTP(response, httptest.NewRequest(method, "/job_lags", nil))
		var body map[string]any
		require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
		return response.Code, body
	}

	// Before the first collector round: no snapshot time, no lag yet.
	code, body := get(http.MethodGet)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, map[string]any{
		"success": true, "snapshot_at": nil, "collect_interval_ms": float64(10000), "jobs": []any{},
	}, body)

	jobManager.PublishJobLagSnapshot(&ccr.JobLagSnapshot{GeneratedAt: 1234, Jobs: map[string]ccr.JobLag{}})
	_, body = get(http.MethodGet)
	require.Equal(t, float64(1234), body["snapshot_at"])

	// Turning the feature flag off makes the platform fall back to /get_lag.
	featureJobLags = false
	defer func() { featureJobLags = true }()
	code, body = get(http.MethodGet)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, map[string]any{
		"success": false, "error_msg": "job_lags is disabled by feature_job_lags=false",
	}, body)
}

func TestFeaturesDeclareJobLags(t *testing.T) {
	service := NewHttpServer("127.0.0.1", 9190, nil, ccr.NewJobManager(nil, nil, "test-syncer"))
	service.RegisterHandlers()
	response := httptest.NewRecorder()
	service.mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/features", nil))
	require.Equal(t, http.StatusOK, response.Code)

	// The shape the platform decodes (syncerclient.FeaturesResponse).
	var features struct {
		Success bool `json:"success"`
		Flags   []struct {
			Feature string `json:"feature"`
			Value   bool   `json:"value"`
			Default string `json:"default"`
		} `json:"flags"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&features))
	require.True(t, features.Success)
	found := false
	for _, flag := range features.Flags {
		if flag.Feature == "feature_job_lags" {
			found = true
			require.True(t, flag.Value)
			require.Equal(t, "true", flag.Default)
		}
	}
	require.True(t, found, "feature_job_lags is not declared in /features")
}
