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

package schema

import (
	"bytes"
	"encoding/binary"

	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/bucket"
	"go.etcd.io/etcd/server/v3/storage/backend"
)

const (
	revBytesLen = 8
)

var (
	authEnabled  = []byte{1}
	authDisabled = []byte{0}
)

type authBackend struct {
	be backend.Backend
	lg *zap.Logger
}

var _ auth.AuthBackend = (*authBackend)(nil)

func NewAuthBackend(lg *zap.Logger, be backend.Backend) *authBackend {
	return &authBackend{
		be: be,
		lg: lg,
	}
}

func (abe *authBackend) CreateAuthBuckets() {
	tx := abe.be.BatchTx()
	tx.LockOutsideApply()
	defer tx.Unlock()
	tx.UnsafeCreateBucket(bucket.Auth)
	tx.UnsafeCreateBucket(bucket.AuthUsers)
	tx.UnsafeCreateBucket(bucket.AuthRoles)
}

func (abe *authBackend) ForceCommit() {
	abe.be.ForceCommit()
}

func (abe *authBackend) ReadTx() auth.AuthReadTx {
	return &authReadTx{tx: abe.be.ReadTx(), lg: abe.lg}
}

func (abe *authBackend) BatchTx() auth.AuthBatchTx {
	return &authBatchTx{tx: abe.be.BatchTx(), lg: abe.lg}
}

type authReadTx struct {
	tx backend.ReadTx
	lg *zap.Logger
}

type authBatchTx struct {
	tx backend.BatchTx
	lg *zap.Logger
}

var _ auth.AuthReadTx = (*authReadTx)(nil)
var _ auth.AuthBatchTx = (*authBatchTx)(nil)

func (atx *authBatchTx) UnsafeSaveAuthEnabled(enabled bool) {
	if enabled {
		atx.tx.UnsafePut(bucket.Auth, bucket.AuthEnabledKeyName, authEnabled)
	} else {
		atx.tx.UnsafePut(bucket.Auth, bucket.AuthEnabledKeyName, authDisabled)
	}
}

func (atx *authBatchTx) UnsafeSaveAuthRevision(rev uint64) {
	revBytes := make([]byte, revBytesLen)
	binary.BigEndian.PutUint64(revBytes, rev)
	atx.tx.UnsafePut(bucket.Auth, bucket.AuthRevisionKeyName, revBytes)
}

func (atx *authBatchTx) UnsafeReadAuthEnabled() bool {
	arx := &authReadTx{tx: atx.tx, lg: atx.lg}
	return arx.UnsafeReadAuthEnabled()
}

func (atx *authBatchTx) UnsafeReadAuthRevision() uint64 {
	arx := &authReadTx{tx: atx.tx, lg: atx.lg}
	return arx.UnsafeReadAuthRevision()
}

func (atx *authBatchTx) Lock() {
	atx.tx.LockInsideApply()
}

func (atx *authBatchTx) Unlock() {
	atx.tx.Unlock()
	// Calling Commit() for defensive purpose. If the number of pending writes doesn't exceed batchLimit,
	// ReadTx can miss some writes issued by its predecessor BatchTx.
	atx.tx.Commit()
}

func (atx *authReadTx) UnsafeReadAuthEnabled() bool {
	_, vs := atx.tx.UnsafeRange(bucket.Auth, bucket.AuthEnabledKeyName, nil, 0)
	if len(vs) == 1 {
		if bytes.Equal(vs[0], authEnabled) {
			return true
		}
	}
	return false
}

func (atx *authReadTx) UnsafeReadAuthRevision() uint64 {
	_, vs := atx.tx.UnsafeRange(bucket.Auth, bucket.AuthRevisionKeyName, nil, 0)
	if len(vs) != 1 {
		// this can happen in the initialization phase
		return 0
	}
	return binary.BigEndian.Uint64(vs[0])
}

func (atx *authReadTx) Lock() {
	atx.tx.RLock()
}

func (atx *authReadTx) Unlock() {
	atx.tx.RUnlock()
}
