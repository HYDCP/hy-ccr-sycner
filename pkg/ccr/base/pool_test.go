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
package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactDSN(t *testing.T) {
	for _, tc := range []struct {
		dsn, expect string
	}{
		{"root:pw@tcp(10.0.0.1:9030)/", "root:******@tcp(10.0.0.1:9030)/"},
		{"root:p@ss/w:rd@tcp(10.0.0.1:9030)/", "root:******@tcp(10.0.0.1:9030)/"},
		{"root:@tcp(10.0.0.1:9030)/", "root:******@tcp(10.0.0.1:9030)/"},
		// The driver rejects an '@' in the host; the password is still masked.
		{"root:pw@tcp(fe@10.0.0.1:9030)/", "root:******@10.0.0.1:9030)/"},
		{"root:pw@tcp(10.0.0.1:9030)", "root:******@tcp(10.0.0.1:9030)"},
		{"root@tcp(10.0.0.1:9030)/", "root@tcp(10.0.0.1:9030)/"},
		{"tcp(10.0.0.1:9030)/", "tcp(10.0.0.1:9030)/"},
	} {
		require.Equal(t, tc.expect, redactDSN(tc.dsn), "dsn: %s", tc.dsn)
	}
}
