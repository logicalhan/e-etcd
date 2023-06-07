/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package badger

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"strings"
	"sync"

	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto"
	"go.uber.org/zap"

	bolt "go.etcd.io/bbolt"

	"go.etcd.io/etcd/server/v3/interfaces"
)

const (
	concurrentGoRoutinesForStream = 16
	bucketPrefix                  = "buckets/"
	dbPrefix                      = "db/"
)

var (
	txnCounter   int
	txnCounterMu = sync.Mutex{}
)

type BadgerDB struct {
	BadgerDB     *badgerdb.DB
	Dir          string
	ValueDir     string
	FreeListType bolt.FreelistType // no-opts
}

type BadgerSuperSetDB interface {
	interfaces.DB
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	NewStream() *badgerdb.Stream
	Backup(w io.Writer, since uint64) (uint64, error)
}

func NewBadgerDB(db *badgerdb.DB, dir string) BadgerSuperSetDB {
	return &BadgerDB{
		BadgerDB:     db,
		Dir:          dir,
		ValueDir:     dir,
		FreeListType: bolt.FreelistMapType, // dummy value
	}
}

func (b *BadgerDB) Get(key []byte) ([]byte, error) {
	var valCopy []byte
	err := b.BadgerDB.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			fmt.Printf("The answer is: %s\n", val)

			// Copying or parsing val is valid.
			valCopy = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	return valCopy, err
}

func (b *BadgerDB) GetFromBucket(bucket string, key string) []byte {
	fullyQualifiedKey := append([]byte(dbPrefix + bucket + "/" + key))
	val, _ := b.Get(fullyQualifiedKey)
	return val
}

func (b *BadgerDB) Flatten() error {
	return b.BadgerDB.Flatten(concurrentGoRoutinesForStream)
}

func (b *BadgerDB) NewStream() *badgerdb.Stream {
	return b.BadgerDB.NewStream()
}

func (b *BadgerDB) Backup(w io.Writer, since uint64) (uint64, error) {
	return b.BadgerDB.Backup(w, since)
}

func (b *BadgerDB) Put(key []byte, val []byte) error {
	return b.BadgerDB.Update(func(txn *badgerdb.Txn) error {
		err := txn.Set(key, val)
		return err
	})
}

func (b *BadgerDB) Delete(key []byte) error {
	return b.BadgerDB.Update(func(txn *badgerdb.Txn) error {
		err := txn.Delete(key)
		return err
	})
}

func (b *BadgerDB) Path() string {
	return b.Dir
}

func (b *BadgerDB) GoString() string {
	return b.BadgerDB.LevelsToString()
}

func (b *BadgerDB) Buckets() []string {
	buckets := make([]string, 0)
	err := b.BadgerDB.View(func(txn *badgerdb.Txn) error {
		it := txn.NewIterator(badgerdb.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(bucketPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			buckets = append(buckets, string(k[len(bucketPrefix):]))
		}
		return nil
	})
	if err != nil {
		return []string{}
	}
	return buckets
}

func (b *BadgerDB) HasBucket(name string) bool {
	buckets := b.Buckets()
	bucketSet := map[string]struct{}{}
	for _, b := range buckets {
		bucketSet[b] = struct{}{}
	}
	_, ok := bucketSet[name]
	return ok
}

func (b *BadgerDB) Defrag(logger *zap.Logger, dbopts interface{}) error {
	if err := b.BadgerDB.Flatten(concurrentGoRoutinesForStream); err != nil {
		return err
	}
	err := b.BadgerDB.RunValueLogGC(0.5)
	if strings.Contains(err.Error(), "Value log GC attempt didn't result in any cleanup") {
		return nil
	}
	return err
}

func (b *BadgerDB) HashBuckets(ignores func(bucketName, keyName []byte) bool) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	buckets := b.Buckets()

	err := b.BadgerDB.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10000
		it := txn.NewIterator(opts)
		defer it.Close()
		for _, bucket := range buckets {
			prefix := []byte(fmt.Sprintf("%s%s", dbPrefix, bucket))
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				curr := it.Item()
				if ignores != nil && !ignores([]byte(bucket), curr.Key()) {
					v, err := curr.ValueCopy(nil)
					if err != nil {
						return err
					}
					h.Write(curr.Key()[len(prefix):])
					h.Write(v)
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *BadgerDB) DeleteBucket(name []byte) error {
	return b.BadgerDB.Update(func(txn *badgerdb.Txn) error {
		it := txn.NewIterator(badgerdb.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(name); it.ValidForPrefix(name); it.Next() {
			item := it.Item()
			k := item.Key()
			err := txn.Delete(k)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BadgerDB) CreateBucket(name string) {
	if b.HasBucket(name) {
		return
	}
	err := b.BadgerDB.Update(func(txn *badgerdb.Txn) error {
		prefix := []byte(fmt.Sprintf("%s%s", bucketPrefix, name))
		err := txn.Set(prefix, []byte{})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		fmt.Println("got error in making bucket", err)
	}
}

func (b *BadgerDB) String() string {
	return "badgerdb"
}

func (b *BadgerDB) Close() error {
	return b.BadgerDB.Close()
}

func (b *BadgerDB) Begin(writable bool) (interfaces.Tx, error) {
	if txn := b.BadgerDB.NewTransaction(writable); txn == nil {
		return nil, interfaces.ErrTxClosed
	} else {
		return NewBadgerTxn(txn, b, writable), nil
	}
}

func (b *BadgerDB) Size() (size int64) {
	buckets := b.Buckets()
	err := b.BadgerDB.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10000
		it := txn.NewIterator(opts)
		defer it.Close()
		for _, bucket := range buckets {
			prefix := []byte(fmt.Sprintf("%s%s", dbPrefix, bucket))
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				curr := it.Item()
				size += curr.EstimatedSize()
			}
		}
		return nil
	})
	if err != nil {
		return 0
	}
	return
}

func (b *BadgerDB) Update(fn interface{}) error {
	return b.BadgerDB.Update(fn.(func(txn *badgerdb.Txn) error))
}

func (b *BadgerDB) View(fn interface{}) error {
	return b.BadgerDB.View(fn.(func(txn *badgerdb.Txn) error))
}

func (b *BadgerDB) Sync() error {
	return b.BadgerDB.Sync()
}

func (b *BadgerDB) Stats() interface{} {
	return BadgerMetrics{
		BlockCacheMetrics: b.BadgerDB.BlockCacheMetrics(),
		IndexCacheMetrics: b.BadgerDB.IndexCacheMetrics(),
	}
}

type BadgerMetrics struct {
	BlockCacheMetrics *ristretto.Metrics
	IndexCacheMetrics *ristretto.Metrics
}

func (b *BadgerDB) Info() interface{} {
	// todo(logicalhan) figure out what we need here
	return nil
}

func (b *BadgerDB) SetFreelistType(freelistType bolt.FreelistType) {
	b.FreeListType = freelistType
}

func (b *BadgerDB) FreelistType() bolt.FreelistType {
	return b.FreeListType
}

func (b *BadgerDB) DBType() string {
	return "badger"
}

type Tx struct {
	txn      *badgerdb.Txn
	id       int
	db       BadgerSuperSetDB
	writable bool
	bucket   *string
}

func NewBadgerTxn(txn *badgerdb.Txn, db BadgerSuperSetDB, writable bool) *Tx {
	if txn == nil {
		return nil
	}
	txnCounterMu.Lock()
	txnCounter++
	txnCounterMu.Unlock()
	txnid := txnCounter
	return &Tx{
		txn:      txn,
		id:       txnid,
		db:       db,
		writable: writable,
	}
}

func (t *Tx) Check() <-chan error {
	// todo(logicalhan) figure out how to return txn errors
	return nil
}

func (t *Tx) ID() int {
	return t.id
}

func (t *Tx) DB() interfaces.DB {
	return t.db
}

func (t *Tx) Size() int64 {
	// todo(logicalhan) what does size of transaction even mean here?
	return 0
}

func (t *Tx) Writable() bool {
	return t.writable
}

func (t *Tx) Stats() interface{} {
	//TODO implement me
	panic("implement me")
}

func (t *Tx) Bucket(name []byte) interfaces.Bucket {
	// validate that the bucket exists.
	if item, _ := t.txn.Get(append([]byte(bucketPrefix), name...)); item != nil {
		return NewBadgerBucket(name, t.txn, t.db, t.writable)
	}
	return nil
}

func (t *Tx) CreateBucket(name []byte) (interfaces.Bucket, error) {

	fullyQualifiedBucketPath := []byte(bucketPrefix + string(name))
	if _, err := t.txn.Get(fullyQualifiedBucketPath); err != nil {
		if err = t.txn.Set(fullyQualifiedBucketPath, []byte{}); err != nil {
			return nil, err
		}
	}
	return NewBadgerBucket(name, t.txn, t.db, t.writable), nil
}

func (t *Tx) CreateBucketIfNotExists(name []byte) (interfaces.Bucket, error) {
	if t.bucket == nil {
		return NewBadgerBucket(name, t.txn, t.db, t.writable), nil
	} else {
		return NewBadgerBucket(append([]byte(*t.bucket), name...), t.txn, t.db, t.writable), nil
	}
}

func (t *Tx) DeleteBucket(name []byte) error {
	if t.bucket == nil {
		return t.DB().DeleteBucket(name)
	} else {
		return t.DB().DeleteBucket(append([]byte(*t.bucket), name...))
	}
}

func (t *Tx) ForEach(i interface{}) error {
	panic("implement me")
}

func (t *Tx) OnCommit(fn interface{}) {
	t.txn.CommitWith(fn.(func(error)))
}

func (t *Tx) Commit() error {
	return t.txn.Commit()
}

func (t *Tx) Rollback() error {
	return nil
}

func (t *Tx) Copy(w io.Writer) error {
	_, err := t.WriteTo(w)
	return err
}

func (t *Tx) WriteTo(w io.Writer) (int64, error) {
	overflow, err := t.DB().(BadgerSuperSetDB).Backup(w, 0)
	return int64(overflow), err
}

func (t *Tx) CopyFile(path string, mode os.FileMode) error {
	//TODO implement me
	panic("implement me")
}

func (t *Tx) Page(id int) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

type BadgerBucket struct {
	bucketPrefix []byte
	txn          *badgerdb.Txn
	db           BadgerSuperSetDB
	writable     bool
}

func NewBadgerBucket(prefix []byte, txn *badgerdb.Txn, db BadgerSuperSetDB, writable bool) interfaces.Bucket {
	if txn == nil {
		return nil
	}
	return &BadgerBucket{
		bucketPrefix: []byte(dbPrefix + string(prefix) + "/"),
		txn:          txn,
		db:           db,
		writable:     writable,
	}
}

func (b *BadgerBucket) UnsafeRange(key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	txn := b.txn
	opts := &badgerdb.IteratorOptions{
		PrefetchValues: true,
		PrefetchSize:   int(limit),
		Reverse:        false,
		AllVersions:    false,
	}

	if limit <= 0 {
		opts.PrefetchSize = math.MaxInt
	}
	startKey := append(b.bucketPrefix, key...)
	isMatch := func(k []byte) bool {
		kEnding := append(b.bucketPrefix, endKey...)
		return bytes.Compare(k, kEnding) < 0
	}
	if len(endKey) == 0 {
		opts.PrefetchSize = 1
		item, err := txn.Get(startKey)
		if err != nil {
			return
		} else {
			vc, err := item.ValueCopy(nil)
			if err == nil {
				keys = append(keys, startKey[len(b.bucketPrefix):])
				vs = append(vs, vc)
				return
			}
		}
	}
	it := txn.NewIterator(*opts)
	defer it.Close()

	for it.Seek(startKey); it.ValidForPrefix(b.bucketPrefix) && isMatch(it.Item().Key()); it.Next() {
		item := it.Item()
		k := item.Key()

		err := item.Value(func(v []byte) error {
			truncatedKey := k[len(b.bucketPrefix):]
			keys = append(keys, truncatedKey)
			vs = append(vs, v)
			return nil
		})
		if limit == int64(len(keys)) {
			break
		}
		if err != nil {
			panic("oh no couldn't iterate over keys")
		}
	}
	return
}

func (b *BadgerBucket) Tx() interfaces.Tx {
	return NewBadgerTxn(b.txn, b.db, b.writable)
}

func (b *BadgerBucket) Root() interface{} {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerBucket) Writable() bool {
	return b.writable
}

func (b *BadgerBucket) Get(key []byte) []byte {
	val, err := b.db.Get(append(b.bucketPrefix, key...))
	if err != nil {
		return nil
	}
	return val
}

func (b *BadgerBucket) Put(key []byte, value []byte) error {
	fullyQualifiedKey := append(b.bucketPrefix, key...)
	return b.txn.Set(fullyQualifiedKey, value)
}

func (b *BadgerBucket) Delete(key []byte) error {
	fullyQualifiedKey := append(b.bucketPrefix, key...)
	return b.txn.Delete(fullyQualifiedKey)
}

func (b *BadgerBucket) Sequence() uint64 {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerBucket) SetSequence(v uint64) error {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerBucket) NextSequence() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerBucket) ForEach(fn func(k []byte, v []byte) error) error {
	txn := b.txn
	it := txn.NewIterator(badgerdb.DefaultIteratorOptions)
	defer it.Close()
	prefix := b.bucketPrefix
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if e := fn(k[len(prefix):], v); e != nil {
			return e
		}
	}
	return nil
}

func (b *BadgerBucket) ForEachBucket(i interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerBucket) Stats() interface{} {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerBucket) FillPercent() float64 {
	//TODO implement me
	panic("implement me")
}

func (b *BadgerBucket) SetFillPercent(f float64) {
	//TODO implement me
	panic("implement me")
}
