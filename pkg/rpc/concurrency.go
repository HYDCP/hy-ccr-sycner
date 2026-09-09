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
package rpc

import (
	"flag"
	"sort"
	"sync"
)

var (
	FlagMaxIngestConcurrencyPerBackend int64
)

func init() {
	flag.Int64Var(&FlagMaxIngestConcurrencyPerBackend, "max_ingest_concurrency_per_backend", 48,
		"The max concurrency of the binlog ingesting per backend")
}

type ConcurrencyWindow struct {
	mu   *sync.Mutex
	cond *sync.Cond

	id        int64
	inflights int64
}

func newCongestionWindow(id int64) *ConcurrencyWindow {
	mu := &sync.Mutex{}
	return &ConcurrencyWindow{
		mu:        mu,
		cond:      sync.NewCond(mu),
		id:        id,
		inflights: 0,
	}
}

func (cw *ConcurrencyWindow) Acquire() {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	for cw.inflights+1 > FlagMaxIngestConcurrencyPerBackend {
		cw.cond.Wait()
	}
	cw.inflights += 1
}

func (cw *ConcurrencyWindow) Release() {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	if cw.inflights == 0 {
		return
	}

	cw.inflights -= 1
	cw.cond.Signal()
}

type ConcurrencyManager struct {
	windows sync.Map
}

func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{}
}

func (cm *ConcurrencyManager) GetWindow(id int64) *ConcurrencyWindow {
	value, ok := cm.windows.Load(id)
	if !ok {
		window := newCongestionWindow(id)
		value, ok = cm.windows.LoadOrStore(id, window)
	}
	return value.(*ConcurrencyWindow)
}

// AcquireAll acquires the concurrency windows for all given backend ids in a
// globally sorted order, deduplicating duplicate ids, and returns a function
// that releases them in reverse order. Callers should defer the returned
// release function. This prevents ABBA deadlocks when multiple tablets acquire
// overlapping backend windows in different leader/follower roles.
func (cm *ConcurrencyManager) AcquireAll(ids []int64) func() {
	seen := make(map[int64]struct{}, len(ids))
	unique := make([]int64, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })

	windows := make([]*ConcurrencyWindow, 0, len(unique))
	for _, id := range unique {
		w := cm.GetWindow(id)
		w.Acquire()
		windows = append(windows, w)
	}
	return func() {
		for i := len(windows) - 1; i >= 0; i-- {
			windows[i].Release()
		}
	}
}
