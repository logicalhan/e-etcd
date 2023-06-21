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
	"encoding/json"
	"log"

	"go.uber.org/zap"

	"go.etcd.io/raft/v3/raftpb"

	"go.etcd.io/etcd/server/v3/bucket"
	"go.etcd.io/etcd/server/v3/storage/backend"
)

// MustUnsafeSaveConfStateToBackend persists confState using given transaction (tx).
// confState in backend is persisted since etcd v3.5.
func MustUnsafeSaveConfStateToBackend(lg *zap.Logger, tx backend.BatchTx, confState *raftpb.ConfState) {
	println("AHN AKHAG ", confState.String())
	confStateBytes, err := json.Marshal(confState)
	if err != nil {
		lg.Panic("Cannot marshal raftpb.ConfState", zap.Stringer("conf-state", confState), zap.Error(err))
	}
	println("starting length", len(confStateBytes))
	tx.UnsafePut(bucket.Meta, bucket.MetaConfStateName, confStateBytes)
}

// UnsafeConfStateFromBackend retrieves ConfState from the backend.
// Returns nil if confState in backend is not persisted (e.g. backend writen by <v3.5).
func UnsafeConfStateFromBackend(lg *zap.Logger, tx backend.ReadTx) *raftpb.ConfState {
	keys, vals := tx.UnsafeRange(bucket.Meta, bucket.MetaConfStateName, nil, 0)
	if len(keys) == 0 {
		return nil
	}

	if len(keys) != 1 {
		for i, k := range keys {
			lg.Info("key", zap.String("key", string(k)), zap.String("val", string(vals[i])))
		}
		lg.Panic(
			"unexpected number of key: "+string(bucket.MetaConfStateName)+" when getting cluster version from backend",
			zap.Int("number-of-key", len(keys)),
		)
	}
	if len(keys) == 1 && bytes.Equal(keys[0], bucket.MetaConsistentIndexKeyName) {
		return nil
	}
	for i, k := range keys {
		println("alalala", string(k))
		lg.Info("key", zap.String("key", string(k)), zap.String("val", string(vals[i])))
	}

	var confState raftpb.ConfState
	println("received length", len(vals[0]))
	if err := json.Unmarshal(vals[0], &confState); err != nil {
		log.Panic("Cannot unmarshal confState json retrieved from the backend",
			zap.ByteString("conf-state-json", vals[0]),
			zap.Error(err))
	}
	return &confState
}
