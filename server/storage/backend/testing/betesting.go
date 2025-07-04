// Copyright 2021 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package betesting

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

func NewTmpBackendFromCfg(t testing.TB, bcfg *backend.BackendConfig) (backend.Backend, string) {
	dir, err := os.MkdirTemp(t.TempDir(), "etcd_backend_test")
	if err != nil {
		panic(err)
	}
	tmpPath := filepath.Join(dir, "database")
	bcfg.Path = tmpPath
	bcfg.Logger = zaptest.NewLogger(t)
	return backend.New(*bcfg), tmpPath
}

// NewTmpBoltBackend creates a backend implementation for testing.
func NewTmpBoltBackend(t testing.TB, batchInterval time.Duration, batchLimit int) (backend.Backend, string) {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
	bcfg.BatchInterval, bcfg.BatchLimit = batchInterval, batchLimit
	return NewTmpBackendFromCfg(t, &bcfg)
}

// NewTmpBadgerBackend creates a backend implementation for testing.
func NewTmpBadgerBackend(t testing.TB, batchInterval time.Duration, batchLimit int) (backend.Backend, string) {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
	bcfg.BatchInterval, bcfg.BatchLimit = batchInterval, batchLimit
	bcfg.DBType = &backend.BadgerDB
	return NewTmpBackendFromCfg(t, &bcfg)
}

func NewTmpSqliteBackend(t testing.TB, batchInterval time.Duration, batchLimit int) (backend.Backend, string) {
	bcfg := backend.DefaultSqliteConfig(zaptest.NewLogger(t))
	bcfg.BatchInterval, bcfg.BatchLimit = batchInterval, batchLimit
	return NewTmpBackendFromCfg(t, &bcfg)
}

func NewDefaultBadgerTmpBackend(t testing.TB) (backend.Backend, string) {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
	bcfg.DBType = &backend.BadgerDB
	return NewTmpBackendFromCfg(t, &bcfg)
}

func NewDefaultSqliteTmpBackend(t testing.TB) (backend.Backend, string) {
	bcfg := backend.DefaultSqliteConfig(zaptest.NewLogger(t))
	return NewTmpBackendFromCfg(t, &bcfg)
}

func NewDefaultBoltTmpBackend(t testing.TB) (backend.Backend, string) {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
	bcfg.DBType = &backend.BoltDB
	return NewTmpBackendFromCfg(t, &bcfg)
}

func Close(t testing.TB, b backend.Backend) {
	assert.NoError(t, b.Close())
}
