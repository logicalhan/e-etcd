// Copyright 2015 The etcd Authors
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

package cindex

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/client/pkg/v3/testutil"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// TestConsistentIndex ensures that LoadConsistentIndex/Save/ConsistentIndex and backend.BatchTx can work well together.
func TestConsistentIndex(t *testing.T) {

	b1, tmpPath1 := betesting.NewTmpBoltBackend(t, time.Nanosecond, 10)
	b2, tmpPath2 := betesting.NewTmpBadgerBackend(t, time.Nanosecond, 10)
	backends := []backend.Backend{b1, b2}
	tmppaths := []string{tmpPath1, tmpPath2}
	for i, be := range backends {
		t.Run(fmt.Sprintf("TestConsistentIndex[db=%s]", be.DBType()), func(t *testing.T) {
			ci := NewConsistentIndex(be)

			tx := be.BatchTx()
			if tx == nil {
				t.Fatal("batch tx is nil")
			}
			tx.Lock()

			schema.UnsafeCreateMetaBucket(tx)
			tx.Unlock()
			be.ForceCommit()
			r := uint64(7890123)
			term := uint64(234)
			ci.SetConsistentIndex(r, term)
			index := ci.ConsistentIndex()
			if index != r {
				t.Errorf("expected %d,got %d", r, index)
			}
			tx.Lock()
			ci.UnsafeSave(tx)
			tx.Unlock()
			be.ForceCommit()
			be.Close()
			beType := be.DBType()
			b := backend.NewDefaultBackend(zaptest.NewLogger(t), tmppaths[i], &beType)
			defer b.Close()
			ci.SetBackend(b)
			index = ci.ConsistentIndex()
			assert.Equal(t, r, index)

			ci = NewConsistentIndex(b)
			index = ci.ConsistentIndex()
			assert.Equal(t, r, index)
		})
	}
}

func TestConsistentIndexDecrease(t *testing.T) {
	testutil.BeforeTest(t)
	initIndex := uint64(100)
	initTerm := uint64(10)

	tcs := []struct {
		name          string
		index         uint64
		term          uint64
		panicExpected bool
	}{
		{
			name:          "Decrease term",
			index:         initIndex + 1,
			term:          initTerm - 1,
			panicExpected: false, // TODO: Change in v3.7
		},
		{
			name:          "Decrease CI",
			index:         initIndex - 1,
			term:          initTerm + 1,
			panicExpected: true,
		},
		{
			name:          "Decrease CI and term",
			index:         initIndex - 1,
			term:          initTerm - 1,
			panicExpected: true,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			be, tmpPath := betesting.NewTmpBoltBackend(t, time.Microsecond, 10)
			tx := be.BatchTx()
			tx.Lock()
			schema.UnsafeCreateMetaBucket(tx)
			schema.UnsafeUpdateConsistentIndex(tx, initIndex, initTerm)
			tx.Unlock()
			be.ForceCommit()
			be.Close()
			betype := be.DBType()
			nbe := backend.NewDefaultBackend(zaptest.NewLogger(t), tmpPath, &betype)
			defer nbe.Close()
			ci := NewConsistentIndex(nbe)
			ci.SetConsistentIndex(tc.index, tc.term)
			tx = nbe.BatchTx()
			func() {
				tx.Lock()
				defer tx.Unlock()
				if tc.panicExpected {
					assert.Panics(t, func() { ci.UnsafeSave(tx) }, "Should refuse to decrease cindex")
					return
				}
				ci.UnsafeSave(tx)
			}()
			if !tc.panicExpected {
				assert.Equal(t, tc.index, ci.ConsistentIndex())

				ci = NewConsistentIndex(nbe)
				assert.Equal(t, tc.index, ci.ConsistentIndex())
			}
		})
	}
}

func TestFakeConsistentIndex(t *testing.T) {

	r := rand.Uint64()
	ci := NewFakeConsistentIndex(r)
	index := ci.ConsistentIndex()
	if index != r {
		t.Errorf("expected %d,got %d", r, index)
	}
	r = rand.Uint64()
	ci.SetConsistentIndex(r, 5)
	index = ci.ConsistentIndex()
	if index != r {
		t.Errorf("expected %d,got %d", r, index)
	}

}
