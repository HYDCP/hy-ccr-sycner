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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrencyManagerAcquireAllAvoidsDeadlock verifies that AcquireAll
// prevents ABBA deadlocks even when two goroutines need the same backends in
// opposite roles and the per-backend concurrency limit is 1.
func TestConcurrencyManagerAcquireAllAvoidsDeadlock(t *testing.T) {
	orig := FlagMaxIngestConcurrencyPerBackend
	FlagMaxIngestConcurrencyPerBackend = 1
	defer func() { FlagMaxIngestConcurrencyPerBackend = orig }()

	cm := NewConcurrencyManager()

	var wg sync.WaitGroup
	wg.Add(2)

	var passCount atomic.Int32
	start := make(chan struct{})

	acquire := func(ids ...int64) {
		defer wg.Done()
		<-start
		release := cm.AcquireAll(ids)
		passCount.Add(1)
		release()
	}

	// Goroutine A treats backend 1 as leader and backend 2 as follower.
	go acquire(1, 2)
	// Goroutine B treats backend 2 as leader and backend 1 as follower.
	go acquire(2, 1)

	// Start both goroutines as close to simultaneously as possible to maximize
	// the chance of hitting an ABBA pattern if AcquireAll were unordered.
	close(start)

	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()

	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("AcquireAll deadlocked with max concurrency 1")
	}

	if passCount.Load() != 2 {
		t.Fatalf("expected both goroutines to pass, got %d", passCount.Load())
	}
}

// TestConcurrencyManagerAcquireAllDeduplicates verifies that AcquireAll only
// acquires one window per duplicate backend id.
func TestConcurrencyManagerAcquireAllDeduplicates(t *testing.T) {
	orig := FlagMaxIngestConcurrencyPerBackend
	FlagMaxIngestConcurrencyPerBackend = 1
	defer func() { FlagMaxIngestConcurrencyPerBackend = orig }()

	cm := NewConcurrencyManager()

	release := cm.AcquireAll([]int64{1, 1, 1})
	defer release()

	w := cm.GetWindow(1)
	if w.inflights != 1 {
		t.Fatalf("expected inflights=1 after deduplication, got %d", w.inflights)
	}
}
