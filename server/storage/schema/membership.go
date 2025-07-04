// Copyright 2016 The etcd Authors
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
	"encoding/json"
	"fmt"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/bucket"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/version"
	"go.etcd.io/etcd/server/v3/storage/backend"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
)

const (
	MemberAttributesSuffix     = "attributes"
	MemberRaftAttributesSuffix = "raftAttributes"
)

type membershipBackend struct {
	lg *zap.Logger
	be backend.Backend
}

func NewMembershipBackend(lg *zap.Logger, be backend.Backend) *membershipBackend {
	return &membershipBackend{
		lg: lg,
		be: be,
	}
}

func (s *membershipBackend) MustSaveMemberToBackend(m *membership.Member) {
	mkey := bucket.BackendMemberKey(m.ID)
	mvalue, err := json.Marshal(m)
	if err != nil {
		s.lg.Panic("failed to marshal member", zap.Error(err))
	}

	tx := s.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	tx.UnsafePut(bucket.Members, mkey, mvalue)
}

// TrimClusterFromBackend removes all information about cluster (versions)
// from the v3 backend.
func (s *membershipBackend) TrimClusterFromBackend() error {
	tx := s.be.BatchTx()
	tx.LockOutsideApply()
	defer tx.Unlock()
	tx.UnsafeDeleteBucket(bucket.Cluster)
	return nil
}

func (s *membershipBackend) MustDeleteMemberFromBackend(id types.ID) {
	mkey := bucket.BackendMemberKey(id)

	tx := s.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	tx.UnsafeDelete(bucket.Members, mkey)
	tx.UnsafePut(bucket.MembersRemoved, mkey, []byte("removed"))
}

func (s *membershipBackend) MustReadMembersFromBackend() (map[types.ID]*membership.Member, map[types.ID]bool) {
	members, removed, err := s.readMembersFromBackend()
	if err != nil {
		s.lg.Panic("couldn't read members from backend", zap.Error(err))
	}
	return members, removed
}

func (s *membershipBackend) readMembersFromBackend() (map[types.ID]*membership.Member, map[types.ID]bool, error) {
	members := make(map[types.ID]*membership.Member)
	removed := make(map[types.ID]bool)

	tx := s.be.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	err := tx.UnsafeForEach(bucket.Members, func(k, v []byte) error {
		memberId := mustParseMemberIDFromBytes(s.lg, k)
		m := &membership.Member{ID: memberId}
		if err := json.Unmarshal(v, &m); err != nil {
			return err
		}
		members[memberId] = m
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't read members from backend: %v", err)
	}

	err = tx.UnsafeForEach(bucket.MembersRemoved, func(k, v []byte) error {
		memberId := mustParseMemberIDFromBytes(s.lg, k)
		removed[memberId] = true
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't read members_removed from backend: %v", err)
	}
	return members, removed, nil
}

// TrimMembershipFromBackend removes all information about members &
// removed_members from the v3 backend.
func (s *membershipBackend) TrimMembershipFromBackend() error {
	s.lg.Info("Trimming membership information from the backend...")
	tx := s.be.BatchTx()
	tx.LockOutsideApply()
	defer tx.Unlock()
	err := tx.UnsafeForEach(bucket.Members, func(k, v []byte) error {
		tx.UnsafeDelete(bucket.Members, k)
		s.lg.Debug("Removed member from the backend",
			zap.Stringer("member", mustParseMemberIDFromBytes(s.lg, k)))
		return nil
	})
	if err != nil {
		return err
	}
	return tx.UnsafeForEach(bucket.MembersRemoved, func(k, v []byte) error {
		tx.UnsafeDelete(bucket.MembersRemoved, k)
		s.lg.Debug("Removed removed_member from the backend",
			zap.Stringer("member", mustParseMemberIDFromBytes(s.lg, k)))
		return nil
	})
}

// MustSaveClusterVersionToBackend saves cluster version to backend.
// The field is populated since etcd v3.5.
func (s *membershipBackend) MustSaveClusterVersionToBackend(ver *semver.Version) {
	ckey := bucket.ClusterClusterVersionKeyName

	tx := s.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	tx.UnsafePut(bucket.Cluster, ckey, []byte(ver.String()))
}

// MustSaveDowngradeToBackend saves downgrade info to backend.
// The field is populated since etcd v3.5.
func (s *membershipBackend) MustSaveDowngradeToBackend(downgrade *version.DowngradeInfo) {
	dkey := bucket.ClusterDowngradeKeyName
	dvalue, err := json.Marshal(downgrade)
	if err != nil {
		s.lg.Panic("failed to marshal downgrade information", zap.Error(err))
	}
	tx := s.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	tx.UnsafePut(bucket.Cluster, dkey, dvalue)
}

func (s *membershipBackend) MustCreateBackendBuckets() {
	tx := s.be.BatchTx()
	tx.LockOutsideApply()
	defer tx.Unlock()
	tx.UnsafeCreateBucket(bucket.Members)
	tx.UnsafeCreateBucket(bucket.MembersRemoved)
	tx.UnsafeCreateBucket(bucket.Cluster)
}

func mustParseMemberIDFromBytes(lg *zap.Logger, key []byte) types.ID {
	id, err := types.IDFromString(string(key))
	if err != nil {
		lg.Panic("failed to parse member id from key", zap.Error(err))
	}
	return id
}

// ClusterVersionFromBackend reads cluster version from backend.
// The field is populated since etcd v3.5.
func (s *membershipBackend) ClusterVersionFromBackend() *semver.Version {
	ckey := bucket.ClusterClusterVersionKeyName
	tx := s.be.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	keys, vals := tx.UnsafeRange(bucket.Cluster, ckey, nil, 0)
	if len(keys) == 0 {
		return nil
	}
	if len(keys) != 1 {
		s.lg.Panic(
			"unexpected number of keys when getting cluster version from backend",
			zap.Int("number-of-key", len(keys)),
		)
	}
	return semver.Must(semver.NewVersion(string(vals[0])))
}

// DowngradeInfoFromBackend reads downgrade info from backend.
// The field is populated since etcd v3.5.
func (s *membershipBackend) DowngradeInfoFromBackend() *version.DowngradeInfo {
	dkey := bucket.ClusterDowngradeKeyName
	tx := s.be.ReadTx()
	tx.Lock()
	defer tx.Unlock()
	keys, vals := tx.UnsafeRange(bucket.Cluster, dkey, nil, 0)
	if len(keys) == 0 {
		return nil
	}

	if len(keys) != 1 {
		s.lg.Panic(
			"unexpected number of keys when getting cluster version from backend",
			zap.Int("number-of-key", len(keys)),
		)
	}
	var d version.DowngradeInfo
	if err := json.Unmarshal(vals[0], &d); err != nil {
		s.lg.Panic("failed to unmarshal downgrade information", zap.Error(err))
	}

	// verify the downgrade info from backend
	if d.Enabled {
		if _, err := semver.NewVersion(d.TargetVersion); err != nil {
			s.lg.Panic(
				"unexpected version format of the downgrade target version from backend",
				zap.String("target-version", d.TargetVersion),
			)
		}
	}
	return &d
}
