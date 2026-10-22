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
	"errors"
	"strings"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// frontendsTestMeta answers GetFrontends with a fixed result; other meta calls are unused.
type frontendsTestMeta struct {
	Metaer
	frontends []*base.Frontend
	err       error
}

func (m *frontendsTestMeta) GetFrontends() ([]*base.Frontend, error) {
	return m.frontends, m.err
}

func TestUpdateFrontendsLogOmitsPassword(t *testing.T) {
	const srcPassword = "src-s3cret-Passw0rd"
	const destPassword = "dest-s3cret-Passw0rd"

	newSpec := func(host, password string) base.Spec {
		return base.Spec{
			Frontend: base.Frontend{Host: host, Port: "9030", ThriftPort: "9020"},
			User:     "ccr_user",
			Password: password,
			Database: "db1",
		}
	}

	for _, tc := range []struct {
		side    string
		srcErr  error
		destErr error
		host    string
	}{
		{side: "src", srcErr: errors.New("src fe down"), host: "10.0.0.1"},
		{side: "dest", destErr: errors.New("dest fe down"), host: "10.0.0.2"},
	} {
		t.Run(tc.side, func(t *testing.T) {
			job := &Job{
				Name:     "job1",
				Src:      newSpec("10.0.0.1", srcPassword),
				Dest:     newSpec("10.0.0.2", destPassword),
				srcMeta:  &frontendsTestMeta{frontends: []*base.Frontend{{Host: "10.0.0.1"}}, err: tc.srcErr},
				destMeta: &frontendsTestMeta{err: tc.destErr},
			}

			hook := logtest.NewGlobal()
			defer hook.Reset()

			require.Error(t, job.updateFrontends())

			var failureLogged bool
			for _, entry := range hook.AllEntries() {
				require.NotContains(t, entry.Message, srcPassword, "log: %s", entry.Message)
				require.NotContains(t, entry.Message, destPassword, "log: %s", entry.Message)
				if strings.HasPrefix(entry.Message, "get "+tc.side+" frontends failed") {
					failureLogged = true
					require.Contains(t, entry.Message, "host: "+tc.host, "log: %s", entry.Message)
				}
			}
			require.True(t, failureLogged, "the %s frontends failure is not logged", tc.side)
		})
	}
}
