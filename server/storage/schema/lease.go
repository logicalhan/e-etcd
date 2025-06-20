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
	"encoding/binary"
	"fmt"

	"go.etcd.io/etcd/server/v3/bucket"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	"go.etcd.io/etcd/server/v3/storage/backend"
)

func UnsafeCreateLeaseBucket(tx backend.BatchTx) {
	tx.UnsafeCreateBucket(bucket.Lease)
}

func MustUnsafeGetAllLeases(tx backend.ReadTx) []*leasepb.Lease {
	ls := make([]*leasepb.Lease, 0)
	err := tx.UnsafeForEach(bucket.Lease, func(k, v []byte) error {
		var lpb leasepb.Lease
		err := lpb.Unmarshal(v)
		if err != nil {
			return fmt.Errorf("failed to Unmarshal lease proto item; lease ID=%016x", bytesToLeaseID(k))
		}
		ls = append(ls, &lpb)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return ls
}

func MustUnsafePutLease(tx backend.BatchTx, lpb *leasepb.Lease) {
	key := leaseIdToBytes(lpb.ID)

	val, err := lpb.Marshal()
	if err != nil {
		panic("failed to marshal lease proto item")
	}
	tx.UnsafePut(bucket.Lease, key, val)
}

func UnsafeDeleteLease(tx backend.BatchTx, lpb *leasepb.Lease) {
	tx.UnsafeDelete(bucket.Lease, leaseIdToBytes(lpb.ID))
}

func MustUnsafeGetLease(tx backend.BatchTx, leaseID int64) *leasepb.Lease {
	_, vs := tx.UnsafeRange(bucket.Lease, leaseIdToBytes(leaseID), nil, 0)
	if len(vs) != 1 {
		return nil
	}
	var lpb leasepb.Lease
	err := lpb.Unmarshal(vs[0])
	if err != nil {
		panic("failed to unmarshal lease proto item")
	}
	return &lpb
}

func leaseIdToBytes(n int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(n))
	return bytes
}

func bytesToLeaseID(bytes []byte) int64 {
	if len(bytes) != 8 {
		panic(fmt.Errorf("lease ID must be 8-byte"))
	}
	return int64(binary.BigEndian.Uint64(bytes))
}
