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
package base_test

import (
	"fmt"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/stretchr/testify/require"
)

const testPassword = "s3cret-Passw0rd"

func TestSpecFormatOmitsPassword(t *testing.T) {
	spec := base.Spec{
		Frontend: base.Frontend{Host: "10.0.0.1", Port: "9030", ThriftPort: "9020"},
		User:     "ccr_user",
		Password: testPassword,
		Database: "db1",
		Table:    "tbl1",
	}

	// Specs are logged by value (e.g. Job.Src), by pointer, and as fields of
	// other structs (e.g. CreateCcrRequest).
	args := map[string]any{
		"value":   spec,
		"pointer": &spec,
		"field":   struct{ Src base.Spec }{Src: spec},
	}
	for name, arg := range args {
		for _, verb := range []string{"%s", "%v", "%+v"} {
			out := fmt.Sprintf(verb, arg)
			require.NotContains(t, out, testPassword, "%s of spec %s: %s", verb, name, out)
			require.Contains(t, out, "host: 10.0.0.1", "%s of spec %s: %s", verb, name, out)
			require.Contains(t, out, "user: ccr_user", "%s of spec %s: %s", verb, name, out)
		}
	}
}

func TestSpecConnectErrorOmitsPassword(t *testing.T) {
	// GetMysqlDB only reports the DSN when the mysql driver rejects it, e.g.
	// when the host contains an '@'.
	spec := base.Spec{
		Frontend: base.Frontend{Host: "fe@10.0.0.1", Port: "9030", ThriftPort: "9020"},
		User:     "ccr_user",
		Password: testPassword,
	}

	_, err := spec.Connect()
	require.Error(t, err)
	for _, verb := range []string{"%v", "%+v"} {
		out := fmt.Sprintf(verb, err)
		require.NotContains(t, out, testPassword, "%s of error: %s", verb, out)
		require.Contains(t, out, "connect to mysql failed, dsn: ccr_user:", "%s of error: %s", verb, out)
	}
}
