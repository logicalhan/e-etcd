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
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/bucket"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
)

func TestUpgradeDowngrade(t *testing.T) {
	tcs := []struct {
		name                      string
		change                    schemaChange
		expectStateAfterUpgrade   map[string]string
		expectStateAfterDowngrade map[string]string
	}{
		{
			name:                    "addNewField empty",
			change:                  addNewField(bucket.Meta, []byte("/test"), []byte("1")),
			expectStateAfterUpgrade: map[string]string{"/test": "1"},
		},
	}
	b1, _ := betesting.NewTmpBoltBackend(t, time.Hour, 10000)
	b2, _ := betesting.NewTmpBadgerBackend(t, time.Hour, 10000)
	backends := []backend.Backend{b1, b2}
	for _, b := range backends {
		for _, tc := range tcs {
			t.Run(tc.name+"["+string(b.DBType())+"]", func(t *testing.T) {
				tx := b.BatchTx()
				if tx == nil {
					t.Fatal("batch tx is nil")
				}
				tx.Lock()
				defer tx.Unlock()
				UnsafeCreateMetaBucket(tx)

				_, err := tc.change.upgradeAction().unsafeDo(tx)
				if err != nil {
					t.Errorf("Failed to upgrade, err: %v", err)
				}
				assertBucketState(t, tx, bucket.Meta, tc.expectStateAfterUpgrade)
				_, err = tc.change.downgradeAction().unsafeDo(tx)
				if err != nil {
					t.Errorf("Failed to downgrade, err: %v", err)
				}
				assertBucketState(t, tx, bucket.Meta, tc.expectStateAfterDowngrade)
			})
		}
	}
}
