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
package storage_test

import (
	"fmt"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNewMysqlDBOpenErrorOmitsPassword(t *testing.T) {
	const password = "s3cret-Passw0rd"

	// sql.Open only fails when the mysql driver rejects the DSN, e.g. when the
	// host contains an '@', so no server is needed.
	_, err := storage.NewMysqlDB("meta@10.0.0.1", 3306, "ccr_user", password, "ccr")
	require.Error(t, err)
	for _, verb := range []string{"%v", "%+v"} {
		out := fmt.Sprintf(verb, err)
		require.NotContains(t, out, password, "%s of error: %s", verb, out)
		require.Contains(t, out, "mysql: open ccr_user@tcp(meta@10.0.0.1:3306) failed", "%s of error: %s", verb, out)
	}
}
