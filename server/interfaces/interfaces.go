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

package interfaces

import (
	"io"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

type DB interface {
	Path() string
	GoString() string
	Buckets() []string
	HasBucket(name string) bool
	DeleteBucket(name []byte) error
	CreateBucket(string)
	String() string
	Close() error
	Begin(writable bool) (Tx, error)
	Update(fn interface{}) error
	View(fn interface{}) error
	Sync() error
	Stats() interface{}
	Info() interface{}
	SetFreelistType(freelistType bolt.FreelistType)
	FreelistType() bolt.FreelistType
	DBType() string
}

type Tx interface {
	Check() <-chan error
	ID() int
	DB() DB
	Size() int64
	Writable() bool
	Cursor() Cursor
	Stats() interface{}
	Bucket(name []byte) Bucket
	CreateBucket(name []byte) (Bucket, error)
	CreateBucketIfNotExists(name []byte) (Bucket, error)
	DeleteBucket(name []byte) error
	ForEach(interface{}) error
	OnCommit(interface{})
	Commit() error
	Rollback() error
	Copy(w io.Writer) error
	WriteTo(w io.Writer) (n int64, err error)
	CopyFile(path string, mode os.FileMode) error
	Page(id int) (interface{}, error)
}

type Bucket interface {
	Tx() Tx
	Root() interface{}
	Writable() bool
	Cursor() Cursor
	Bucket(name []byte) Bucket
	CreateBucket(key []byte) (Bucket, error)
	CreateBucketIfNotExists(key []byte) (Bucket, error)
	DeleteBucket(key []byte) error
	Get(key []byte) []byte
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Sequence() uint64
	SetSequence(v uint64) error
	NextSequence() (uint64, error)
	ForEach(interface{}) error
	ForEachBucket(interface{}) error
	Stats() interface{}
	FillPercent() float64
	SetFillPercent(float64)
}

type BucketStats interface {
	Add(other BucketStats)
}
type Cursor interface {
	Bucket() Bucket
	First() (key []byte, value []byte)
	Last() (key []byte, value []byte)
	Next() (key []byte, value []byte)
	Prev() (key []byte, value []byte)
	Seek(seek []byte) (key []byte, value []byte)
	Delete() error
}
type TxStats interface {
	Sub(other TxStats) TxStats
	GetPageCount() int64
	IncPageCount(delta int64) int64
	GetPageAlloc() int64
	IncPageAlloc(delta int64) int64
	GetCursorCount() int64
	IncCursorCount(delta int64) int64
	GetNodeCount() int64
	IncNodeCount(delta int64) int64
	GetNodeDeref() int64
	IncNodeDeref(delta int64) int64
	GetRebalance() int64
	IncRebalance(delta int64) int64
	GetRebalanceTime() time.Duration
	IncRebalanceTime(delta time.Duration) time.Duration
	GetSplit() int64
	IncSplit(delta int64) int64
	GetSpill() int64
	IncSpill(delta int64) int64
	GetSpillTime() time.Duration
	IncSpillTime(delta time.Duration) time.Duration
	GetWrite() int64
	IncWrite(delta int64) int64
	GetWriteTime() time.Duration
	IncWriteTime(delta time.Duration) time.Duration
}
type Stats interface {
	Sub(other Stats) Stats
}
