// Copyright 2022 The etcd Authors
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

package mvcc

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/bucket"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
)

// TestScheduledCompact ensures that UnsafeSetScheduledCompact&UnsafeReadScheduledCompact work well together.
func TestScheduledCompact(t *testing.T) {
	tcs := []struct {
		value int64
	}{
		{
			value: 1,
		},
		{
			value: 0,
		},
		{
			value: math.MaxInt64,
		},
		{
			value: math.MinInt64,
		},
	}
	for _, tc := range tcs {
		b1, p1 := betesting.NewTmpBoltBackend(t, time.Microsecond, 10)
		b2, p2 := betesting.NewTmpBadgerBackend(t, time.Microsecond, 10)
		backends := []backend.Backend{b1, b2}
		paths := []string{p1, p2}
		for i, be := range backends {
			t.Run(fmt.Sprint(tc.value)+":"+string(be.DBType()), func(t *testing.T) {
				lg := zaptest.NewLogger(t)
				tx := be.BatchTx()
				if tx == nil {
					t.Fatal("batch tx is nil")
				}
				tx.Lock()
				tx.UnsafeCreateBucket(bucket.Meta)
				UnsafeSetScheduledCompact(tx, tc.value)
				tx.Unlock()
				be.ForceCommit()
				be.Close()
				dbtype := be.DBType()
				b := backend.NewDefaultBackend(lg, paths[i], &dbtype)
				defer b.Close()
				v, found := UnsafeReadScheduledCompact(b.BatchTx())
				assert.Equal(t, true, found)
				assert.Equal(t, tc.value, v)
			})
		}
	}
}

// TestFinishedCompact ensures that UnsafeSetFinishedCompact&UnsafeReadFinishedCompact work well together.
func TestFinishedCompact(t *testing.T) {
	tcs := []struct {
		value int64
	}{
		{
			value: 1,
		},
		{
			value: 0,
		},
		{
			value: math.MaxInt64,
		},
		{
			value: math.MinInt64,
		},
	}
	for _, tc := range tcs {
		b1, p1 := betesting.NewTmpBoltBackend(t, time.Microsecond, 10)
		b2, p2 := betesting.NewTmpBadgerBackend(t, time.Microsecond, 10)
		backends := []backend.Backend{b1, b2}
		paths := []string{p1, p2}
		for i, be := range backends {
			t.Run(fmt.Sprint(tc.value)+":"+string(be.DBType()), func(t *testing.T) {
				lg := zaptest.NewLogger(t)
				tx := be.BatchTx()
				if tx == nil {
					t.Fatal("batch tx is nil")
				}
				tx.Lock()
				tx.UnsafeCreateBucket(bucket.Meta)
				UnsafeSetFinishedCompact(tx, tc.value)
				tx.Unlock()
				be.ForceCommit()
				be.Close()
				dbtype := be.DBType()
				b := backend.NewDefaultBackend(lg, paths[i], &dbtype)
				defer b.Close()
				v, found := UnsafeReadFinishedCompact(b.BatchTx())
				assert.Equal(t, true, found)
				assert.Equal(t, tc.value, v)
			})
		}
	}
}
