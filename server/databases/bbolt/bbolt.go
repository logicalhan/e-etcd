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

package bbolt

import (
	"io"
	"os"

	bolt "go.etcd.io/bbolt"

	"go.etcd.io/etcd/server/v3/interfaces"
)

type DbOpts struct {
	MMapSize     int
	FreelistType interface{}
	NoSync       bool
	NoGrowSync   bool
	Mlock        bool
}

func SetOptions(opts DbOpts) *bolt.Options {
	bopts := &bolt.Options{}
	if boltOpenOptions != nil {
		*bopts = *boltOpenOptions
	}
	bopts.InitialMmapSize = opts.MMapSize
	if opts.FreelistType != nil {
		bopts.FreelistType = opts.FreelistType.(bolt.FreelistType)
	}
	bopts.NoSync = opts.NoSync
	bopts.NoGrowSync = opts.NoGrowSync
	bopts.Mlock = opts.Mlock
	return bopts
}
func Open(path string, mode os.FileMode, options *bolt.Options) (interfaces.DB, error) {
	if db, err := bolt.Open(path, mode, options); err != nil {
		return nil, err
	} else {
		return &BBoltDB{db: db}, nil
	}
}

type BBoltDB struct {
	db *bolt.DB
}

func (b *BBoltDB) Path() string {
	return b.db.Path()
}

func (b *BBoltDB) DBType() string {
	return "bolt"
}

func (b *BBoltDB) GoString() string {
	return b.db.GoString()
}

func (b *BBoltDB) String() string {
	return b.db.String()
}

func (b *BBoltDB) Close() error {
	return b.db.Close()
}

// Buckets no-opt
func (b *BBoltDB) Buckets() []string {
	return nil
}

// DeleteBucket no-opt
func (b *BBoltDB) DeleteBucket(name []byte) error {
	return nil
}

// HasBucket no-opt
func (b *BBoltDB) HasBucket(name string) bool {
	return false
}

// CreateBucket no-opt
func (b *BBoltDB) CreateBucket(name string) {
	return
}

func (b *BBoltDB) Begin(writable bool) (interfaces.Tx, error) {
	btx, err := b.db.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &BBoltTx{tx: btx}, nil
}

func (b *BBoltDB) Update(fn interface{}) error {
	return b.db.Update(fn.(func(*bolt.Tx) error))
}

func (b *BBoltDB) View(fn interface{}) error {
	return b.db.View(fn.(func(*bolt.Tx) error))
}

func (b *BBoltDB) Sync() error {
	return b.db.Sync()
}

func (b *BBoltDB) Stats() interface{} {
	return b.db.Stats()
}

func (b *BBoltDB) Info() interface{} {
	return b.db.Info()
}

func (b *BBoltDB) FreelistType() bolt.FreelistType {
	return b.db.FreelistType
}

func (b *BBoltDB) SetFreelistType(freelistType bolt.FreelistType) {
	b.db.FreelistType = freelistType
}

type BBoltTx struct {
	tx *bolt.Tx
}

func (b *BBoltTx) Check() <-chan error {
	return b.tx.Check()
}

func (b *BBoltTx) ID() int {
	return b.tx.ID()
}

func (b *BBoltTx) DB() interfaces.DB {
	return &BBoltDB{db: b.tx.DB()}
}

func (b *BBoltTx) Size() int64 {
	return b.tx.Size()
}

func (b *BBoltTx) Writable() bool {
	return b.tx.Writable()
}

func (b *BBoltTx) Cursor() interfaces.Cursor {
	if b.tx.Cursor() != nil {
		return &BBoltCursor{b.tx.Cursor()}
	}
	return nil
}

func (b *BBoltTx) Stats() interface{} {
	return b.tx.Stats()
}

func (b *BBoltTx) Bucket(name []byte) interfaces.Bucket {
	if buck := b.tx.Bucket(name); buck != nil {
		return &BBoltBucket{b.tx.Bucket(name)}
	} else {
		return nil
	}
}

func (b *BBoltTx) CreateBucket(name []byte) (interfaces.Bucket, error) {
	bbuck, err := b.tx.CreateBucket(name)
	if err != nil {
		return nil, err
	}
	return &BBoltBucket{bbuck}, nil
}

func (b *BBoltTx) CreateBucketIfNotExists(name []byte) (interfaces.Bucket, error) {
	if buck, err := b.tx.CreateBucketIfNotExists(name); err != nil {
		return nil, err
	} else {
		return &BBoltBucket{buck}, nil
	}
}

func (b *BBoltTx) DeleteBucket(name []byte) error {
	return b.tx.DeleteBucket(name)
}

func (b *BBoltTx) ForEach(fn interface{}) error {
	return b.tx.ForEach(fn.(func(name []byte, b *bolt.Bucket) error))
}

func (b *BBoltTx) OnCommit(fn interface{}) {
	b.tx.OnCommit(fn.(func()))
}

func (b *BBoltTx) Commit() error {
	return b.tx.Commit()
}

func (b *BBoltTx) Rollback() error {
	return b.tx.Rollback()
}

func (b *BBoltTx) Copy(w io.Writer) error {
	return b.tx.Copy(w)
}

func (b *BBoltTx) WriteTo(w io.Writer) (n int64, err error) {
	return b.tx.WriteTo(w)
}

func (b *BBoltTx) CopyFile(path string, mode os.FileMode) error {
	return b.tx.CopyFile(path, mode)
}

func (b *BBoltTx) Page(id int) (interface{}, error) {
	return b.tx.Page(id)
}

type BBoltCursor struct {
	cursor *bolt.Cursor
}

func (b *BBoltCursor) Bucket() interfaces.Bucket {
	if buck := b.cursor.Bucket(); buck != nil {
		return &BBoltBucket{bucket: buck}
	}
	return nil
}

func (b *BBoltCursor) First() (key []byte, value []byte) {
	return b.cursor.First()
}

func (b *BBoltCursor) Last() (key []byte, value []byte) {
	return b.cursor.Last()
}

func (b *BBoltCursor) Next() (key []byte, value []byte) {
	return b.cursor.Next()
}

func (b *BBoltCursor) Prev() (key []byte, value []byte) {
	return b.cursor.Prev()
}

func (b *BBoltCursor) Seek(seek []byte) (key []byte, value []byte) {
	return b.cursor.Seek(seek)
}

func (b *BBoltCursor) Delete() error {
	return b.cursor.Delete()
}

type BBoltBucket struct {
	bucket *bolt.Bucket
}

func (b *BBoltBucket) Tx() interfaces.Tx {
	if btx := b.bucket.Tx(); btx != nil {
		return &BBoltTx{tx: btx}
	}
	return nil
}

func (b *BBoltBucket) Root() interface{} {
	return b.bucket.Root()
}

func (b *BBoltBucket) Writable() bool {
	return b.bucket.Writable()
}

func (b *BBoltBucket) Cursor() interfaces.Cursor {
	if curs := b.bucket.Cursor(); curs != nil {
		return &BBoltCursor{cursor: curs}
	}
	return nil
}

func (b *BBoltBucket) Bucket(name []byte) interfaces.Bucket {
	if buck := b.bucket.Bucket(name); buck != nil {
		return &BBoltBucket{bucket: buck}
	}
	return nil
}

func (b *BBoltBucket) FillPercent() float64 {
	return b.bucket.FillPercent
}

func (b *BBoltBucket) SetFillPercent(fp float64) {
	b.bucket.FillPercent = fp
}
func (b *BBoltBucket) CreateBucket(key []byte) (interfaces.Bucket, error) {
	if buck, err := b.bucket.CreateBucket(key); err != nil {
		return nil, err
	} else {
		return &BBoltBucket{bucket: buck}, nil
	}
}

func (b *BBoltBucket) CreateBucketIfNotExists(key []byte) (interfaces.Bucket, error) {
	if buck, err := b.bucket.CreateBucketIfNotExists(key); err != nil {
		return nil, err
	} else {
		return &BBoltBucket{bucket: buck}, nil
	}
}

func (b *BBoltBucket) DeleteBucket(key []byte) error {
	return b.bucket.DeleteBucket(key)
}

func (b *BBoltBucket) Get(key []byte) []byte {
	return b.bucket.Get(key)
}

func (b *BBoltBucket) Put(key []byte, value []byte) error {
	return b.bucket.Put(key, value)
}

func (b *BBoltBucket) Delete(key []byte) error {
	return b.bucket.Delete(key)
}

func (b *BBoltBucket) Sequence() uint64 {
	return b.bucket.Sequence()
}

func (b *BBoltBucket) SetSequence(v uint64) error {
	return b.bucket.SetSequence(v)
}

func (b *BBoltBucket) NextSequence() (uint64, error) {
	return b.bucket.NextSequence()
}

func (b *BBoltBucket) ForEach(fn interface{}) error {
	return b.bucket.ForEach(fn.(func(k []byte, v []byte) error))
}

func (b *BBoltBucket) ForEachBucket(fn interface{}) error {
	return b.bucket.ForEachBucket(fn.(func(k []byte) error))
}

func (b *BBoltBucket) Stats() interface{} {
	return b.bucket.Stats()
}

//db, err := bolt.Open(bcfg.Path, 0600, bopts)
