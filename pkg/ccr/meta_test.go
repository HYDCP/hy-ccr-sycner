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
	"sync"
	"testing"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
)

func newTestBackend(id int64, host string, bePort uint16) *base.Backend {
	return &base.Backend{
		Id:       id,
		Host:     host,
		BePort:   bePort,
		HttpPort: bePort + 1,
		BrpcPort: bePort + 2,
	}
}

func TestReplaceBackendsCacheSwapsEntries(t *testing.T) {
	meta := NewMeta(&base.Spec{})

	first := []*base.Backend{newTestBackend(1, "10.0.0.1", 9060), newTestBackend(2, "10.0.0.2", 9060)}
	if err := meta.replaceBackendsCache(first, "show backends"); err != nil {
		t.Fatalf("replace backends cache failed: %v", err)
	}
	if got := len(meta.Backends); got != 2 {
		t.Fatalf("expected 2 cached backends, got %d", got)
	}
	if !meta.isBackendsCacheValid() {
		t.Fatal("expected the cache to be valid right after a refresh")
	}

	// A scale-in must drop the removed backend from both indexes.
	second := []*base.Backend{newTestBackend(2, "10.0.0.2", 9060)}
	if err := meta.replaceBackendsCache(second, "show backends"); err != nil {
		t.Fatalf("replace backends cache failed: %v", err)
	}
	if _, ok := meta.Backends[1]; ok {
		t.Fatal("expected the removed backend to be dropped from the cache")
	}
	if _, ok := meta.lookupBackendId("10.0.0.1:9060"); ok {
		t.Fatal("expected the removed backend to be dropped from the host:port index")
	}
	if id, ok := meta.lookupBackendId("10.0.0.2:9060"); !ok || id != 2 {
		t.Fatalf("expected backend id 2 for 10.0.0.2:9060, got id=%d ok=%v", id, ok)
	}
}

func TestReplaceBackendsCacheKeepsPreviousOnEmptyResult(t *testing.T) {
	meta := NewMeta(&base.Spec{})

	backends := []*base.Backend{newTestBackend(1, "10.0.0.1", 9060)}
	if err := meta.replaceBackendsCache(backends, "show backends"); err != nil {
		t.Fatalf("replace backends cache failed: %v", err)
	}

	if err := meta.replaceBackendsCache(nil, "show backends"); err == nil {
		t.Fatal("expected an empty backend list to be reported as a failed refresh")
	}

	if got := len(meta.Backends); got != 1 {
		t.Fatalf("expected the previous cache to be kept, got %d backends", got)
	}
	if id, ok := meta.lookupBackendId("10.0.0.1:9060"); !ok || id != 1 {
		t.Fatalf("expected the previous host:port index to be kept, got id=%d ok=%v", id, ok)
	}
	if !meta.isBackendsCacheValid() {
		t.Fatal("expected the previous cache to stay usable after a failed refresh")
	}
}

func TestInvalidateBackendsCache(t *testing.T) {
	meta := NewMeta(&base.Spec{})

	if err := meta.replaceBackendsCache([]*base.Backend{newTestBackend(1, "10.0.0.1", 9060)}, "show backends"); err != nil {
		t.Fatalf("replace backends cache failed: %v", err)
	}

	meta.InvalidateBackendsCache()
	if meta.isBackendsCacheValid() {
		t.Fatal("expected the cache to be invalid after InvalidateBackendsCache")
	}

	// The entries stay readable until the next successful refresh replaces them.
	if id, ok := meta.lookupBackendId("10.0.0.1:9060"); !ok || id != 1 {
		t.Fatalf("expected invalidation to keep the entries readable, got id=%d ok=%v", id, ok)
	}
}

func TestGetBackendMapReturnsSnapshot(t *testing.T) {
	meta := NewMeta(&base.Spec{})

	if err := meta.replaceBackendsCache([]*base.Backend{newTestBackend(1, "10.0.0.1", 9060)}, "show backends"); err != nil {
		t.Fatalf("replace backends cache failed: %v", err)
	}

	backendMap, err := meta.GetBackendMap()
	if err != nil {
		t.Fatalf("get backend map failed: %v", err)
	}
	delete(backendMap, 1)

	if _, ok := meta.Backends[1]; !ok {
		t.Fatal("expected the cache to be unaffected by mutations of the returned map")
	}
}

// TestBackendsCacheConcurrentAccess is meant to be run under -race: refreshing
// the cache must not be observable as a partially filled map by readers.
func TestBackendsCacheConcurrentAccess(t *testing.T) {
	meta := NewMeta(&base.Spec{})
	if err := meta.replaceBackendsCache([]*base.Backend{newTestBackend(1, "10.0.0.1", 9060)}, "show backends"); err != nil {
		t.Fatalf("replace backends cache failed: %v", err)
	}

	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			backends := make([]*base.Backend, 0, 8)
			for id := int64(1); id <= 8; id++ {
				backends = append(backends, newTestBackend(id, "10.0.0.1", uint16(9060+id)))
			}
			if err := meta.replaceBackendsCache(backends, "show backends"); err != nil {
				t.Errorf("replace backends cache failed: %v", err)
				return
			}
			if i%3 == 0 {
				meta.InvalidateBackendsCache()
			}
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				meta.lookupBackendId("10.0.0.1:9061")
				meta.isBackendsCacheValid()
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	close(done)
	wg.Wait()
}

// withBackendsCacheTTL sets the package level TTL for one test and restores it.
func withBackendsCacheTTL(t *testing.T, ttl time.Duration) {
	t.Helper()
	prev := GetBackendsCacheTTL()
	if err := SetBackendsCacheTTL(ttl); err != nil {
		t.Fatalf("set backends cache ttl %v failed: %v", ttl, err)
	}
	t.Cleanup(func() {
		if err := SetBackendsCacheTTL(prev); err != nil {
			t.Fatalf("restore backends cache ttl %v failed: %v", prev, err)
		}
	})
}

// TestBackendsCacheManualModeNeverExpires covers ttl=0: an old cache stays valid,
// and reads are served from it without querying FE. The empty Spec has no FE to
// connect to, so a refresh attempt would make GetBackends/GetBackendMap fail.
func TestBackendsCacheManualModeNeverExpires(t *testing.T) {
	withBackendsCacheTTL(t, 0)

	meta := NewMeta(&base.Spec{})
	if err := meta.replaceBackendsCache([]*base.Backend{newTestBackend(1, "10.0.0.1", 9060)}, "show backends"); err != nil {
		t.Fatalf("replace backends cache failed: %v", err)
	}
	meta.backendsLastUpdate = time.Now().Add(-365 * 24 * time.Hour)

	if !meta.isBackendsCacheValid() {
		t.Fatal("expected the cache to stay valid in manual mode (ttl=0)")
	}
	if backends, err := meta.GetBackends(); err != nil || len(backends) != 1 {
		t.Fatalf("expected GetBackends to be served from cache, got %v, err=%v", backends, err)
	}
	if backendMap, err := meta.GetBackendMap(); err != nil || len(backendMap) != 1 {
		t.Fatalf("expected GetBackendMap to be served from cache, got %v, err=%v", backendMap, err)
	}

	meta.InvalidateBackendsCache()
	if meta.isBackendsCacheValid() {
		t.Fatal("expected InvalidateBackendsCache to invalidate the cache in manual mode")
	}
}

func TestBackendsCacheAutoModeExpires(t *testing.T) {
	withBackendsCacheTTL(t, 30*time.Second)

	meta := NewMeta(&base.Spec{})
	if err := meta.replaceBackendsCache([]*base.Backend{newTestBackend(1, "10.0.0.1", 9060)}, "show backends"); err != nil {
		t.Fatalf("replace backends cache failed: %v", err)
	}
	if !meta.isBackendsCacheValid() {
		t.Fatal("expected a fresh cache to be valid")
	}

	meta.backendsLastUpdate = time.Now().Add(-31 * time.Second)
	if meta.isBackendsCacheValid() {
		t.Fatal("expected the cache to expire after the configured ttl")
	}
}

// TestBackendsCacheEmptyIsInvalid: the first use always loads from FE, in both modes.
func TestBackendsCacheEmptyIsInvalid(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Hour} {
		withBackendsCacheTTL(t, ttl)
		meta := NewMeta(&base.Spec{})
		meta.backendsLastUpdate = time.Now()
		if meta.isBackendsCacheValid() {
			t.Fatalf("expected an empty cache to be invalid, ttl=%v", ttl)
		}
	}
}

func TestSetBackendsCacheTTL(t *testing.T) {
	withBackendsCacheTTL(t, 0)

	if err := SetBackendsCacheTTL(-time.Second); err == nil {
		t.Fatal("expected a negative ttl to be rejected")
	}
	if got := GetBackendsCacheTTL(); got != 0 {
		t.Fatalf("expected a rejected ttl to leave the setting unchanged, got %v", got)
	}

	if err := SetBackendsCacheTTL(60 * time.Second); err != nil {
		t.Fatalf("set ttl 60s failed: %v", err)
	}
	if got := GetBackendsCacheTTL(); got != 60*time.Second {
		t.Fatalf("expected ttl 60s, got %v", got)
	}
}
