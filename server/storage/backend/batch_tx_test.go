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

package backend_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	bucket2 "go.etcd.io/etcd/server/v3/bucket"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
)

func TestBatchTxPut(t *testing.T) {
	b1, _ := betesting.NewTmpBoltBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b1)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b2)
	b3, _ := betesting.NewTmpSqliteBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b3)
	backends := []backend.Backend{b1, b2, b3}
	for _, b := range backends {

		tx := b.BatchTx()

		tx.Lock()

		// create bucket
		tx.UnsafeCreateBucket(bucket2.Test)

		// put
		v := []byte("bar")
		tx.UnsafePut(bucket2.Test, []byte("foo"), v)

		tx.Unlock()

		// check put result before and after tx is committed
		for k := 0; k < 2; k++ {
			tx := b.BatchTx()
			tx.Lock()
			_, gv := tx.UnsafeRange(bucket2.Test, []byte("foo"), nil, 0)

			tx.Unlock()
			if len(gv) == 0 {
				t.Fatalf("no results back from Unsafe Range")
			}
			if !reflect.DeepEqual(gv[0], v) {
				t.Errorf("v = %s, want %s", string(gv[0]), string(v))
			}
			tx.Commit()
		}
	}
}

func TestBatchTxRange(t *testing.T) {
	b1, _ := betesting.NewTmpBoltBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b1)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b2)
	backends := []backend.Backend{b1, b2}
	for _, b := range backends {

		tx := b.BatchTx()
		tx.Lock()
		defer tx.Unlock()

		tx.UnsafeCreateBucket(bucket2.Test)
		// put keys
		allKeys := [][]byte{[]byte("foo"), []byte("foo1"), []byte("foo2")}
		allVals := [][]byte{[]byte("bar"), []byte("bar1"), []byte("bar2")}
		for i := range allKeys {
			tx.UnsafePut(bucket2.Test, allKeys[i], allVals[i])
		}

		tests := []struct {
			key    []byte
			endKey []byte
			limit  int64

			wkeys [][]byte
			wvals [][]byte
		}{
			// single key
			{
				[]byte("foo"), nil, 0,
				allKeys[:1], allVals[:1],
			},
			// single key, bad
			{
				[]byte("doo"), nil, 0,
				nil, nil,
			},
			// key range
			{
				[]byte("foo"), []byte("foo1"), 0,
				allKeys[:1], allVals[:1],
			},
			// key range, get all keys
			{
				[]byte("foo"), []byte("foo3"), 0,
				allKeys, allVals,
			},
			// key range, bad
			{
				[]byte("goo"), []byte("goo3"), 0,
				nil, nil,
			},
			// key range with effective limit
			{
				[]byte("foo"), []byte("foo3"), 1,
				allKeys[:1], allVals[:1],
			},
			// key range with limit
			{
				[]byte("foo"), []byte("foo3"), 4,
				allKeys, allVals,
			},
		}
		for i, tt := range tests {
			ks, vs := tx.UnsafeRange(bucket2.Test, tt.key, tt.endKey, tt.limit)
			keys := []string{}
			vals := []string{}
			for j, k := range ks {
				keys = append(keys, string(k))
				vals = append(vals, string(vs[j]))
			}
			wkeys := []string{}
			for _, k := range tt.wkeys {
				wkeys = append(wkeys, string(k))
			}
			wvals := []string{}
			for _, k := range tt.wvals {
				wvals = append(wvals, string(k))
			}

			if !reflect.DeepEqual(keys, wkeys) {
				t.Errorf("#%d: keys = %+v, want %+v", i, keys, wkeys)
			}
			if !reflect.DeepEqual(vals, wvals) {
				t.Errorf("#%d: vals = %+v, want %+v", i, vals, wvals)
			}
		}
	}
}

func TestBatchTxDelete(t *testing.T) {
	b1, _ := betesting.NewTmpBoltBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b1)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b2)
	backends := []backend.Backend{b1, b2}
	for _, b := range backends {

		tx := b.BatchTx()
		tx.Lock()

		tx.UnsafeCreateBucket(bucket2.Test)
		tx.UnsafePut(bucket2.Test, []byte("foo"), []byte("bar"))

		tx.UnsafeDelete(bucket2.Test, []byte("foo"))

		tx.Unlock()

		// check put result before and after tx is committed
		for k := 0; k < 2; k++ {
			tx.Lock()
			ks, _ := tx.UnsafeRange(bucket2.Test, []byte("foo"), nil, 0)
			tx.Unlock()
			if len(ks) != 0 {
				keys := []string{}
				for _, k := range ks {
					keys = append(keys, string(k))
				}
				t.Errorf("keys on foo = %v, want nil", keys)
			}
			tx.Commit()
		}
	}
}

func TestBatchTxCommit(t *testing.T) {
	b1, _ := betesting.NewTmpBoltBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b1)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b2)
	backends := []backend.Backend{b1, b2}
	for _, b := range backends {
		expectedVal := []byte("bar")
		tx := b.BatchTx()
		tx.Lock()
		tx.UnsafeCreateBucket(bucket2.Test)
		tx.UnsafePut(bucket2.Test, []byte("foo"), expectedVal)
		tx.Unlock()

		tx.Commit()
		val := backend.DbFromBackendForTest(b).GetFromBucket(string(bucket2.Test.Name()), "foo")
		if !bytes.Equal(val, expectedVal) {
			t.Errorf("got %s, want %s", val, expectedVal)
		}
	}
}

func TestBatchTxBatchLimitCommit(t *testing.T) {
	// start backend with batch limit 1 so one write can
	// trigger a commit
	b1, _ := betesting.NewTmpBoltBackend(t, time.Nanosecond, 1)
	defer betesting.Close(t, b1)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Nanosecond, 1)
	defer betesting.Close(t, b2)
	backends := []backend.Backend{b1, b2}
	for _, b := range backends {
		t.Run(fmt.Sprintf("TestBatchTxBatchLimitCommit[db=%s]", b.DBType()), func(t *testing.T) {
			expectedVal := []byte("bar")
			tx := b.BatchTx()
			tx.Lock()
			tx.UnsafeCreateBucket(bucket2.Test)
			tx.UnsafePut(bucket2.Test, []byte("foo"), expectedVal)
			tx.Unlock()

			val := backend.DbFromBackendForTest(b).GetFromBucket(string(bucket2.Test.Name()), "foo")
			if !bytes.Equal(val, expectedVal) {
				t.Errorf("got %s, want %s", val, expectedVal)
			}
		})

	}
}
