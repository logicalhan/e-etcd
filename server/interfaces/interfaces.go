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

	"go.uber.org/zap"

	bolt "go.etcd.io/bbolt"
)

type DB interface {
	Path() string
	GoString() string
	Buckets() []string
	HasBucket(name string) bool
	DeleteBucket(name []byte) error
	CreateBucket(string)
	GetFromBucket(bucket string, key string) []byte
	String() string
	Close() error
	Begin(writable bool) (Tx, error)
	Size() (size int64)
	Sync() error
	Stats() interface{}
	Info() interface{}
	SetFreelistType(freelistType bolt.FreelistType)
	FreelistType() bolt.FreelistType
	DBType() string
	HashBuckets(ignores func(bucketName, keyName []byte) bool) (uint32, error)
	Defrag(logger *zap.Logger, dbopts interface{}) error
}

type Tx interface {
	DB() DB
	Size() int64
	Writable() bool
	Stats() interface{}
	Bucket(name []byte) Bucket
	CreateBucket(name []byte) (Bucket, error)
	DeleteBucket(name []byte) error
	ForEach(interface{}) error
	Commit() error
	Rollback() error
	WriteTo(w io.Writer) (n int64, err error)
}

type Bucket interface {
	Tx() Tx
	Root() interface{}
	Writable() bool
	Get(key []byte) []byte
	Put(key []byte, value []byte) error
	UnsafeRange(key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte)
	Delete(key []byte) error
	ForEach(func(k []byte, v []byte) error) error
	Stats() interface{}
	SetFillPercent(float64)
}
