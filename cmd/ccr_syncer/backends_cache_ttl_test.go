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
package main

import (
	"flag"
	"testing"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/stretchr/testify/require"
)

func TestBackendsCacheTTLFlagDefaultsToManual(t *testing.T) {
	f := flag.Lookup("backends_cache_ttl")
	require.NotNil(t, f, "the -backends_cache_ttl flag must be registered")
	require.Equal(t, "0s", f.DefValue)
}

func TestApplyBackendsCacheTTL(t *testing.T) {
	prev := ccr.GetBackendsCacheTTL()
	t.Cleanup(func() { require.NoError(t, ccr.SetBackendsCacheTTL(prev)) })

	require.NoError(t, applyBackendsCacheTTL(0))
	require.Equal(t, time.Duration(0), ccr.GetBackendsCacheTTL())

	require.NoError(t, applyBackendsCacheTTL(60*time.Second))
	require.Equal(t, 60*time.Second, ccr.GetBackendsCacheTTL())

	require.Error(t, applyBackendsCacheTTL(-time.Second))
	require.Equal(t, 60*time.Second, ccr.GetBackendsCacheTTL(), "a rejected ttl must not change the setting")
}
