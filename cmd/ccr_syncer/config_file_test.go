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
	"os"
	"path/filepath"
	"strings"
	"testing"

	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestParseConfigFileLogOmitsPassword(t *testing.T) {
	const password = "s3cret-Passw0rd"

	saved := syncer
	defer func() { syncer = saved }()

	syncer.Config_file = filepath.Join(t.TempDir(), "ccr.conf")
	content := "db_user=ccr_user\ndb_password=" + password + "\n"
	require.NoError(t, os.WriteFile(syncer.Config_file, []byte(content), 0o600))

	hook := logtest.NewGlobal()
	defer hook.Reset()

	require.NoError(t, parseConfigFile())
	require.Equal(t, "ccr_user", syncer.Db_user)
	require.Equal(t, password, syncer.Db_password)

	var passwordLogged bool
	for _, entry := range hook.AllEntries() {
		require.NotContains(t, entry.Message, password, "log: %s", entry.Message)
		if strings.HasPrefix(entry.Message, "force set config db_password=") {
			passwordLogged = true
		}
	}
	require.True(t, passwordLogged, "the db_password override is not logged")
}
