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
