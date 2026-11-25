// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License

package rpc

import (
	"errors"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	festruct_types "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/types"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

// fakeFeClient only implements the methods used by the tests, calling any
// other method of the embedded nil IFeRpc panics.
type fakeFeClient struct {
	IFeRpc
	addr      string
	calls     int
	getBinlog func() (*festruct.TGetBinlogResult_, error)
}

func (c *fakeFeClient) Address() string {
	return c.addr
}

func (c *fakeFeClient) GetBinlog(*base.Spec, int64, int64) (*festruct.TGetBinlogResult_, error) {
	c.calls++
	return c.getBinlog()
}

func notMasterResult(master *festruct_types.TNetworkAddress) *festruct.TGetBinlogResult_ {
	return &festruct.TGetBinlogResult_{
		Status:        &tstatus.TStatus{StatusCode: tstatus.TStatusCode_NOT_MASTER},
		MasterAddress: master,
	}
}

func newTestFeRpc(master IFeRpc, others ...IFeRpc) *FeRpc {
	clients := map[string]IFeRpc{master.Address(): master}
	for _, client := range others {
		clients[client.Address()] = client
	}
	return &FeRpc{
		spec:          &base.Spec{},
		masterClient:  master,
		clients:       clients,
		cachedFeAddrs: map[string]bool{},
	}
}

// A FE which is not ready (master election in progress, meta out of date)
// answers NOT_MASTER without the master address. It is a transient state, the
// error must not be a panic error, otherwise the job never syncs again.
func TestNotMasterWithoutMasterAddressIsRecoverable(t *testing.T) {
	follower := &fakeFeClient{addr: "follower:9020"}
	follower.getBinlog = func() (*festruct.TGetBinlogResult_, error) {
		return notMasterResult(nil), nil
	}
	feRpc := newTestFeRpc(follower)

	_, err := feRpc.GetBinlog(&base.Spec{}, 1, 1)
	if err == nil {
		t.Fatalf("expect an error, the fe is not master")
	}
	if !errors.Is(err, ErrFeNotMasterCompatible) {
		t.Fatalf("expect ErrFeNotMasterCompatible, got: %+v", err)
	}

	var xerr *xerror.XError
	if !errors.As(err, &xerr) {
		t.Fatalf("expect an xerror, got: %+v", err)
	}
	if xerr.IsPanic() {
		t.Fatalf("NOT_MASTER without master address must be recoverable, got panic error: %+v", err)
	}
	if !xerr.IsRecoverable() {
		t.Fatalf("expect a recoverable error, got: %+v", err)
	}
}

// Once the FE is ready and reports the master address, the next call is
// redirected to the new master and succeeds.
func TestRedirectToMasterAfterFeBecomesReady(t *testing.T) {
	master := &fakeFeClient{addr: "master:9020"}
	master.getBinlog = func() (*festruct.TGetBinlogResult_, error) {
		return &festruct.TGetBinlogResult_{
			Status: &tstatus.TStatus{StatusCode: tstatus.TStatusCode_OK},
		}, nil
	}

	ready := false
	follower := &fakeFeClient{addr: "follower:9020"}
	follower.getBinlog = func() (*festruct.TGetBinlogResult_, error) {
		if !ready {
			return notMasterResult(nil), nil
		}
		return notMasterResult(&festruct_types.TNetworkAddress{Hostname: "master", Port: 9020}), nil
	}

	// the follower is the only known fe, like a job created with one fe address
	feRpc := newTestFeRpc(follower)

	if _, err := feRpc.GetBinlog(&base.Spec{}, 1, 1); err == nil {
		t.Fatalf("expect an error, the follower is not ready")
	}
	if master.calls != 0 {
		t.Fatalf("master should not be called, calls: %d", master.calls)
	}

	// the follower is ready now, register the master client to avoid a real connection
	ready = true
	feRpc.addClient(master)

	resp, err := feRpc.GetBinlog(&base.Spec{}, 1, 1)
	if err != nil {
		t.Fatalf("expect redirect to master and succeed, got: %+v", err)
	}
	if resp.GetStatus().GetStatusCode() != tstatus.TStatusCode_OK {
		t.Fatalf("expect OK, got: %s", resp.GetStatus().GetStatusCode())
	}
	if master.calls != 1 {
		t.Fatalf("expect master called once, calls: %d", master.calls)
	}
	if feRpc.getMasterClient().Address() != master.Address() {
		t.Fatalf("expect master client switched to %s, got: %s", master.Address(), feRpc.getMasterClient().Address())
	}
}
