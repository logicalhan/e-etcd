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
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap/zaptest"

	bucket2 "go.etcd.io/etcd/server/v3/bucket"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
)

func TestBackendClose(t *testing.T) {

	b1, _ := betesting.NewTmpBoltBackend(t, time.Hour, 10000)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Hour, 10000)
	b3, _ := betesting.NewTmpSqliteBackend(t, time.Hour, 10000)
	backends := []backend.Backend{b1, b2, b3}
	for _, b := range backends {
		// check close could work
		done := make(chan struct{}, 1)
		go func() {
			err := b.Close()
			if err != nil {
				t.Errorf("close error = %v, want nil", err)
			}
			done <- struct{}{}
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("failed to close database in 10s")
		}
	}
}

func TestBackendSnapshot(t *testing.T) {
	b, _ := betesting.NewTmpSqliteBackend(t, time.Hour, 10000)
	defer betesting.Close(t, b)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket2.Test)
	tx.UnsafePut(bucket2.Test, []byte("foo"), []byte("bar"))
	tx.UnsafePut(bucket2.Test, []byte("foo1"), []byte("bar1"))
	tx.UnsafePut(bucket2.Test, []byte("foo2"), []byte("bar2"))
	tx.Unlock()
	b.ForceCommit()
	oldtx := b.BatchTx()
	oldtx.Lock()
	oldks, _ := oldtx.UnsafeRange(bucket2.Test, []byte("foo"), []byte("goo"), 0)
	if len(oldks) != 1 {
		t.Errorf("len(kvs) = %d, want 1", len(oldks))
	}
	oldtx.Unlock()
	oldtx.Commit()
	// write snapshot to a new file
	f, err := os.CreateTemp(t.TempDir(), "etcd_backend_test")
	if err != nil {
		t.Fatal(err)
	}
	snap := b.Snapshot()
	defer func() { assert.NoError(t, snap.Close()) }()
	if _, err := snap.WriteTo(f); err != nil {
		t.Fatal(err)
	}
	stat, _ := f.Stat()
	println(stat.Size())
	assert.NoError(t, f.Close())

	// bootstrap new backend from the snapshot
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
	println("f.Name()", f.Name())
	bcfg.Path, bcfg.BatchInterval, bcfg.BatchLimit = f.Name(), time.Hour, 10000
	bcfg.DBType = &backend.SQLite
	nb := backend.New(bcfg)
	defer betesting.Close(t, nb)

	newTx := nb.BatchTx()
	newTx.Lock()
	ks, _ := newTx.UnsafeRange(bucket2.Test, []byte("foo"), []byte("goo"), 0)
	if len(ks) != 1 {
		t.Errorf("len(kvs) = %d, want 1", len(ks))
	}
	newTx.Unlock()
}

func TestBackendBatchIntervalCommit(t *testing.T) {
	// start backend with super short batch interval so
	// we do not need to wait long before commit to happen.
	b1, _ := betesting.NewTmpBoltBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b1)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b2)
	b3, _ := betesting.NewTmpSqliteBackend(t, time.Nanosecond, 10000)
	defer betesting.Close(t, b3)
	backends := []backend.Backend{b1, b2, b3}
	for _, b := range backends {
		t.Run(fmt.Sprintf("TestBackendBatchIntervalCommit[db=%s]", b.DBType()), func(t *testing.T) {
			pc := backend.CommitsForTest(b)

			tx := b.BatchTx()
			tx.Lock()
			tx.UnsafeCreateBucket(bucket2.Test)
			tx.UnsafePut(bucket2.Test, []byte("foo"), []byte("bar"))
			tx.Unlock()

			for i := 0; i < 10; i++ {
				if backend.CommitsForTest(b) >= pc+1 {
					break
				}
				time.Sleep(time.Duration(i*100) * time.Millisecond)
			}
			val := backend.DbFromBackendForTest(b).GetFromBucket(string(bucket2.Test.Name()), "foo")
			if val == nil {
				t.Errorf("couldn't find foo in bucket test in backend")
			} else if !bytes.Equal([]byte("bar"), val) {
				t.Errorf("got '%s', want 'bar'", val)
			}
		})
	}
}

func TestBadgerDefrag(t *testing.T) {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
	//Make sure we change BackendFreelistType
	//The goal is to verify that we restore config option after defrag.
	if bcfg.BackendFreelistType == bolt.FreelistMapType {
		bcfg.BackendFreelistType = bolt.FreelistArrayType
	} else {
		bcfg.BackendFreelistType = bolt.FreelistMapType
	}
	bcfg.DBType = &backend.BadgerDB

	b, _ := betesting.NewTmpBackendFromCfg(t, &bcfg)

	defer betesting.Close(t, b)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket2.Test)
	for i := 0; i < backend.DefragLimitForTest()+100; i++ {
		tx.UnsafePut(bucket2.Test, []byte(fmt.Sprintf("foo_%d", i)), []byte("bar"))
	}
	tx.Unlock()
	b.ForceCommit()
	size := b.Size()
	// remove some keys to ensure the disk space will be reclaimed after defrag
	tx = b.BatchTx()
	tx.Lock()
	for i := 0; i < 100; i++ {
		tx.UnsafeDelete(bucket2.Test, []byte(fmt.Sprintf("foo_%d", i)))
	}

	tx.Unlock()
	b.ForceCommit()

	// shrink and check hash
	oh, err := b.Hash(nil)
	if err != nil {
		t.Fatal(err)
	}

	err = b.Defrag()
	if err != nil {
		t.Fatal(err)
	}

	nh, err := b.Hash(nil)
	if err != nil {
		t.Fatal(err)
	}
	if oh != nh {
		t.Errorf("hash = %v, want %v", nh, oh)
	}

	nsize := b.Size()
	if nsize >= size {
		t.Errorf("new size = %v, want < %d", nsize, size)
	}

	db := backend.DbFromBackendForTest(b)
	if db.FreelistType() != bcfg.BackendFreelistType {
		t.Errorf("db FreelistType = [%v], want [%v]", db.FreelistType(), bcfg.BackendFreelistType)
	}

	// try put more keys after shrink.
	tx = b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket2.Test)
	tx.UnsafePut(bucket2.Test, []byte("more"), []byte("bar"))
	tx.Unlock()
	b.ForceCommit()
}

func TestBackendDefrag(t *testing.T) {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
	// Make sure we change BackendFreelistType
	// The goal is to verify that we restore config option after defrag.
	if bcfg.BackendFreelistType == bolt.FreelistMapType {
		bcfg.BackendFreelistType = bolt.FreelistArrayType
	} else {
		bcfg.BackendFreelistType = bolt.FreelistMapType
	}

	b, _ := betesting.NewTmpBackendFromCfg(t, &bcfg)

	defer betesting.Close(t, b)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket2.Test)
	for i := 0; i < backend.DefragLimitForTest()+100; i++ {
		tx.UnsafePut(bucket2.Test, []byte(fmt.Sprintf("foo_%d", i)), []byte("bar"))
	}
	tx.Unlock()
	b.ForceCommit()

	// remove some keys to ensure the disk space will be reclaimed after defrag
	tx = b.BatchTx()
	tx.Lock()
	for i := 0; i < 50; i++ {
		tx.UnsafeDelete(bucket2.Test, []byte(fmt.Sprintf("foo_%d", i)))
	}
	tx.Unlock()
	b.ForceCommit()

	size := b.Size()

	// shrink and check hash
	oh, err := b.Hash(nil)
	if err != nil {
		t.Fatal(err)
	}

	err = b.Defrag()
	if err != nil {
		t.Fatal(err)
	}

	nh, err := b.Hash(nil)
	if err != nil {
		t.Fatal(err)
	}
	if oh != nh {
		t.Errorf("hash = %v, want %v", nh, oh)
	}

	nsize := b.Size()
	if nsize >= size {
		t.Errorf("new size = %v, want < %d", nsize, size)
	}
	db := backend.DbFromBackendForTest(b)
	if db.FreelistType() != bcfg.BackendFreelistType {
		t.Errorf("db FreelistType = [%v], want [%v]", db.FreelistType(), bcfg.BackendFreelistType)
	}

	// try put more keys after shrink.
	tx = b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket2.Test)
	tx.UnsafePut(bucket2.Test, []byte("more"), []byte("bar"))
	tx.Unlock()
	b.ForceCommit()
}

// TestBackendWriteback ensures writes are stored to the read txn on write txn unlock.
func TestBackendWriteback(t *testing.T) {
	b1, _ := betesting.NewTmpBoltBackend(t, time.Hour, 10000)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Hour, 10000)
	backends := []backend.Backend{b1, b2}
	for _, b := range backends {
		tx := b.BatchTx()
		tx.Lock()
		tx.UnsafeCreateBucket(bucket2.Key)
		tx.UnsafePut(bucket2.Key, []byte("abc"), []byte("bar"))
		tx.UnsafePut(bucket2.Key, []byte("def"), []byte("baz"))
		tx.UnsafePut(bucket2.Key, []byte("overwrite"), []byte("1"))
		tx.Unlock()

		// overwrites should be propagated too
		tx.Lock()
		tx.UnsafePut(bucket2.Key, []byte("overwrite"), []byte("2"))
		tx.Unlock()

		keys := []struct {
			key   []byte
			end   []byte
			limit int64

			wkey [][]byte
			wval [][]byte
		}{
			{
				key: []byte("abc"),
				end: nil,

				wkey: [][]byte{[]byte("abc")},
				wval: [][]byte{[]byte("bar")},
			},
			{
				key: []byte("abc"),
				end: []byte("def"),

				wkey: [][]byte{[]byte("abc")},
				wval: [][]byte{[]byte("bar")},
			},
			{
				key: []byte("abc"),
				end: []byte("deg"),

				wkey: [][]byte{[]byte("abc"), []byte("def")},
				wval: [][]byte{[]byte("bar"), []byte("baz")},
			},
			{
				key:   []byte("abc"),
				end:   []byte("\xff"),
				limit: 1,

				wkey: [][]byte{[]byte("abc")},
				wval: [][]byte{[]byte("bar")},
			},
			{
				key: []byte("abc"),
				end: []byte("\xff"),

				wkey: [][]byte{[]byte("abc"), []byte("def"), []byte("overwrite")},
				wval: [][]byte{[]byte("bar"), []byte("baz"), []byte("2")},
			},
		}
		rtx := b.ReadTx()
		for i, tt := range keys {
			func() {
				rtx.RLock()
				defer rtx.RUnlock()
				k, v := rtx.UnsafeRange(bucket2.Key, tt.key, tt.end, tt.limit)
				if !reflect.DeepEqual(tt.wkey, k) || !reflect.DeepEqual(tt.wval, v) {
					t.Errorf("#%d: want k=%+v, v=%+v; got k=%+v, v=%+v", i, tt.wkey, tt.wval, k, v)
				}
			}()
		}
	}
}

// TestConcurrentReadTx ensures that current read transaction can see all prior writes stored in read buffer
func TestConcurrentReadTx(t *testing.T) {
	b1, _ := betesting.NewTmpBoltBackend(t, time.Hour, 10000)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Hour, 10000)
	backends := []backend.Backend{b1, b2}
	for _, b := range backends {

		wtx1 := b.BatchTx()
		wtx1.Lock()
		wtx1.UnsafeCreateBucket(bucket2.Key)
		wtx1.UnsafePut(bucket2.Key, []byte("abc"), []byte("ABC"))
		wtx1.UnsafePut(bucket2.Key, []byte("overwrite"), []byte("1"))
		wtx1.Unlock()

		wtx2 := b.BatchTx()
		wtx2.Lock()
		wtx2.UnsafePut(bucket2.Key, []byte("def"), []byte("DEF"))
		wtx2.UnsafePut(bucket2.Key, []byte("overwrite"), []byte("2"))
		wtx2.Unlock()

		rtx := b.ConcurrentReadTx()
		rtx.RLock() // no-op
		k, v := rtx.UnsafeRange(bucket2.Key, []byte("abc"), []byte("\xff"), 0)
		rtx.RUnlock()
		wKey := [][]byte{[]byte("abc"), []byte("def"), []byte("overwrite")}
		wVal := [][]byte{[]byte("ABC"), []byte("DEF"), []byte("2")}
		if !reflect.DeepEqual(wKey, k) || !reflect.DeepEqual(wVal, v) {
			t.Errorf("want k=%+v, v=%+v; got k=%+v, v=%+v", wKey, wVal, k, v)
		}
	}
}

// TestBackendWritebackForEach checks that partially written / buffered
// data is visited in the same order as fully committed data.
func TestBackendWritebackForEach(t *testing.T) {
	b1, _ := betesting.NewTmpBoltBackend(t, time.Hour, 10000)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Hour, 10000)
	backends := []backend.Backend{b1, b2}
	for _, b := range backends {
		t.Run(fmt.Sprintf("TestBackendWritebackForEach[db=%s]", b.DBType()), func(t *testing.T) {
			tx := b.BatchTx()
			tx.Lock()
			tx.UnsafeCreateBucket(bucket2.Key)
			for i := 0; i < 5; i++ {
				k := []byte(fmt.Sprintf("%04d", i))
				tx.UnsafePut(bucket2.Key, k, []byte("bar"))
			}
			tx.Unlock()

			// writeback
			b.ForceCommit()

			tx.Lock()
			tx.UnsafeCreateBucket(bucket2.Key)
			for i := 5; i < 20; i++ {
				k := []byte(fmt.Sprintf("%04d", i))
				tx.UnsafePut(bucket2.Key, k, []byte("bar"))
			}
			tx.Unlock()

			seq := ""
			getSeq := func(k, v []byte) error {
				seq += string(k)
				return nil
			}
			rtx := b.ConcurrentReadTx()
			rtx.RLock()
			assert.NoError(t, rtx.UnsafeForEach(bucket2.Key, getSeq))
			rtx.RUnlock()

			partialSeq := seq
			t.Logf("partial=%s", partialSeq)
			seq = ""
			b.ForceCommit()

			tx.Lock()
			assert.NoError(t, tx.UnsafeForEach(bucket2.Key, getSeq))
			tx.Unlock()
			t.Logf("full=%s", seq)
			if seq != partialSeq {
				t.Fatalf("expected %q, got %q", seq, partialSeq)
			}
		})
	}
}
