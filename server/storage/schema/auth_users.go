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
	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/server/v3/bucket"
)

func (abe *authBackend) GetUser(username string) *authpb.User {
	tx := abe.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	return tx.UnsafeGetUser(username)
}

func (atx *authBatchTx) UnsafeGetUser(username string) *authpb.User {
	arx := &authReadTx{tx: atx.tx, lg: atx.lg}
	return arx.UnsafeGetUser(username)
}

func (atx *authBatchTx) UnsafeGetAllUsers() []*authpb.User {
	arx := &authReadTx{tx: atx.tx, lg: atx.lg}
	return arx.UnsafeGetAllUsers()
}

func (atx *authBatchTx) UnsafePutUser(user *authpb.User) {
	b, err := user.Marshal()
	if err != nil {
		atx.lg.Panic("failed to unmarshal 'authpb.User'", zap.Error(err))
	}
	atx.tx.UnsafePut(bucket.AuthUsers, user.Name, b)
}

func (atx *authBatchTx) UnsafeDeleteUser(username string) {
	atx.tx.UnsafeDelete(bucket.AuthUsers, []byte(username))
}

func (atx *authReadTx) UnsafeGetUser(username string) *authpb.User {
	_, vs := atx.tx.UnsafeRange(bucket.AuthUsers, []byte(username), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	user := &authpb.User{}
	err := user.Unmarshal(vs[0])
	if err != nil {
		atx.lg.Panic(
			"failed to unmarshal 'authpb.User'",
			zap.String("user-name", username),
			zap.Error(err),
		)
	}
	return user
}

func (abe *authBackend) GetAllUsers() []*authpb.User {
	tx := abe.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	return tx.UnsafeGetAllUsers()
}

func (atx *authReadTx) UnsafeGetAllUsers() []*authpb.User {
	var vs [][]byte
	err := atx.tx.UnsafeForEach(bucket.AuthUsers, func(k []byte, v []byte) error {
		vs = append(vs, v)
		return nil
	})
	if err != nil {
		atx.lg.Panic("failed to get users",
			zap.Error(err))
	}
	if len(vs) == 0 {
		return nil
	}

	users := make([]*authpb.User, len(vs))
	for i := range vs {
		user := &authpb.User{}
		err := user.Unmarshal(vs[i])
		if err != nil {
			atx.lg.Panic("failed to unmarshal 'authpb.User'", zap.Error(err))
		}
		users[i] = user
	}
	return users
}
