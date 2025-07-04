// Copyright 2015 The etcd Authors
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

package etcdserver

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/membershippb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/testutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/client/pkg/v3/verify"
	"go.etcd.io/etcd/pkg/v3/idutil"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	apply2 "go.etcd.io/etcd/server/v3/etcdserver/apply"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/etcdserver/errors"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mock/mockstorage"
	"go.etcd.io/etcd/server/v3/mock/mockstore"
	"go.etcd.io/etcd/server/v3/mock/mockwait"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// TestDoLocalAction tests requests which do not need to go through raft to be applied,
// and are served through local data.
func TestDoLocalAction(t *testing.T) {
	tests := []struct {
		req pb.Request

		wresp    Response
		werr     error
		wactions []testutil.Action
	}{
		{
			pb.Request{Method: "GET", ID: 1, Wait: true},
			Response{Watcher: v2store.NewNopWatcher()}, nil, []testutil.Action{{Name: "Watch"}},
		},
		{
			pb.Request{Method: "GET", ID: 1},
			Response{Event: &v2store.Event{}}, nil,
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
		{
			pb.Request{Method: "HEAD", ID: 1},
			Response{Event: &v2store.Event{}}, nil,
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
		{
			pb.Request{Method: "BADMETHOD", ID: 1},
			Response{}, errors.ErrUnknownMethod, []testutil.Action{},
		},
	}
	for i, tt := range tests {
		st := mockstore.NewRecorder()
		srv := &EtcdServer{
			lgMu:     new(sync.RWMutex),
			lg:       zaptest.NewLogger(t),
			v2store:  st,
			reqIDGen: idutil.NewGenerator(0, time.Time{}),
		}
		resp, err := srv.Do(context.Background(), tt.req)

		if err != tt.werr {
			t.Fatalf("#%d: err = %+v, want %+v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(resp, tt.wresp) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, tt.wresp)
		}
		gaction := st.Action()
		if !reflect.DeepEqual(gaction, tt.wactions) {
			t.Errorf("#%d: action = %+v, want %+v", i, gaction, tt.wactions)
		}
	}
}

// TestDoBadLocalAction tests server requests which do not need to go through consensus,
// and return errors when they fetch from local data.
func TestDoBadLocalAction(t *testing.T) {
	storeErr := fmt.Errorf("bah")
	tests := []struct {
		req pb.Request

		wactions []testutil.Action
	}{
		{
			pb.Request{Method: "GET", ID: 1, Wait: true},
			[]testutil.Action{{Name: "Watch"}},
		},
		{
			pb.Request{Method: "GET", ID: 1},
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
		{
			pb.Request{Method: "HEAD", ID: 1},
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
	}
	for i, tt := range tests {
		st := mockstore.NewErrRecorder(storeErr)
		srv := &EtcdServer{
			lgMu:     new(sync.RWMutex),
			lg:       zaptest.NewLogger(t),
			v2store:  st,
			reqIDGen: idutil.NewGenerator(0, time.Time{}),
		}
		resp, err := srv.Do(context.Background(), tt.req)

		if err != storeErr {
			t.Fatalf("#%d: err = %+v, want %+v", i, err, storeErr)
		}
		if !reflect.DeepEqual(resp, Response{}) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, Response{})
		}
		gaction := st.Action()
		if !reflect.DeepEqual(gaction, tt.wactions) {
			t.Errorf("#%d: action = %+v, want %+v", i, gaction, tt.wactions)
		}
	}
}

// TestApplyRepeat tests that server handles repeat raft messages gracefully
func TestApplyRepeat(t *testing.T) {
	n := newNodeConfChangeCommitterStream()
	n.readyc <- raft.Ready{
		SoftState: &raft.SoftState{RaftState: raft.StateLeader},
	}
	cl := newTestCluster(t, nil)
	st := v2store.New()
	cl.SetStore(v2store.New())
	cl.AddMember(&membership.Member{ID: 1234}, true)
	r := newRaftNode(raftNodeConfig{
		lg:          zaptest.NewLogger(t),
		Node:        n,
		raftStorage: raft.NewMemoryStorage(),
		storage:     mockstorage.NewStorageRecorder(""),
		transport:   newNopTransporter(),
	})
	s := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		lg:           zaptest.NewLogger(t),
		r:            *r,
		v2store:      st,
		cluster:      cl,
		reqIDGen:     idutil.NewGenerator(0, time.Time{}),
		SyncTicker:   &time.Ticker{},
		consistIndex: cindex.NewFakeConsistentIndex(0),
	}
	s.applyV2 = &applierV2store{store: s.v2store, cluster: s.cluster}
	s.start()
	req := &pb.Request{Method: "QGET", ID: uint64(1)}
	ents := []raftpb.Entry{{Index: 1, Data: pbutil.MustMarshal(req)}}
	n.readyc <- raft.Ready{CommittedEntries: ents}
	// dup msg
	n.readyc <- raft.Ready{CommittedEntries: ents}

	// use a conf change to block until dup msgs are all processed
	cc := &raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: 2}
	ents = []raftpb.Entry{{
		Index: 2,
		Type:  raftpb.EntryConfChange,
		Data:  pbutil.MustMarshal(cc),
	}}
	n.readyc <- raft.Ready{CommittedEntries: ents}
	// wait for conf change message
	act, err := n.Wait(1)
	// wait for stop message (async to avoid deadlock)
	stopc := make(chan error, 1)
	go func() {
		_, werr := n.Wait(1)
		stopc <- werr
	}()
	s.Stop()

	// only want to confirm etcdserver won't panic; no data to check

	if err != nil {
		t.Fatal(err)
	}
	if len(act) == 0 {
		t.Fatalf("expected len(act)=0, got %d", len(act))
	}

	if err = <-stopc; err != nil {
		t.Fatalf("error on stop (%v)", err)
	}
}

func TestApplyRequest(t *testing.T) {
	tests := []struct {
		req pb.Request

		wresp    Response
		wactions []testutil.Action
	}{
		// POST ==> Create
		{
			pb.Request{Method: "POST", ID: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", false, "", true, v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// POST ==> Create, with expiration
		{
			pb.Request{Method: "POST", ID: 1, Expiration: 1337},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", false, "", true, v2store.TTLOptionSet{ExpireTime: time.Unix(0, 1337)}},
				},
			},
		},
		// POST ==> Create, with dir
		{
			pb.Request{Method: "POST", ID: 1, Dir: true},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", true, "", true, v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT ==> Set
		{
			pb.Request{Method: "PUT", ID: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Set",
					Params: []interface{}{"", false, "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT ==> Set, with dir
		{
			pb.Request{Method: "PUT", ID: 1, Dir: true},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Set",
					Params: []interface{}{"", true, "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevExist=true ==> Update
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: pbutil.Boolp(true)},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Update",
					Params: []interface{}{"", "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevExist=false ==> Create
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: pbutil.Boolp(false)},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", false, "", false, v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevExist=true *and* PrevIndex set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: pbutil.Boolp(true), PrevIndex: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndSwap",
					Params: []interface{}{"", "", uint64(1), "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevExist=false *and* PrevIndex set ==> Create
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: pbutil.Boolp(false), PrevIndex: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", false, "", false, v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevIndex set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevIndex: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndSwap",
					Params: []interface{}{"", "", uint64(1), "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevValue set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevValue: "bar"},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndSwap",
					Params: []interface{}{"", "bar", uint64(0), "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevIndex and PrevValue set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevIndex: 1, PrevValue: "bar"},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndSwap",
					Params: []interface{}{"", "bar", uint64(1), "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// DELETE ==> Delete
		{
			pb.Request{Method: "DELETE", ID: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Delete",
					Params: []interface{}{"", false, false},
				},
			},
		},
		// DELETE with PrevIndex set ==> CompareAndDelete
		{
			pb.Request{Method: "DELETE", ID: 1, PrevIndex: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndDelete",
					Params: []interface{}{"", "", uint64(1)},
				},
			},
		},
		// DELETE with PrevValue set ==> CompareAndDelete
		{
			pb.Request{Method: "DELETE", ID: 1, PrevValue: "bar"},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndDelete",
					Params: []interface{}{"", "bar", uint64(0)},
				},
			},
		},
		// DELETE with PrevIndex *and* PrevValue set ==> CompareAndDelete
		{
			pb.Request{Method: "DELETE", ID: 1, PrevIndex: 5, PrevValue: "bar"},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndDelete",
					Params: []interface{}{"", "bar", uint64(5)},
				},
			},
		},
		// QGET ==> Get
		{
			pb.Request{Method: "QGET", ID: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
		// SYNC ==> DeleteExpiredKeys
		{
			pb.Request{Method: "SYNC", ID: 1},
			Response{},
			[]testutil.Action{
				{
					Name:   "DeleteExpiredKeys",
					Params: []interface{}{time.Unix(0, 0)},
				},
			},
		},
		{
			pb.Request{Method: "SYNC", ID: 1, Time: 12345},
			Response{},
			[]testutil.Action{
				{
					Name:   "DeleteExpiredKeys",
					Params: []interface{}{time.Unix(0, 12345)},
				},
			},
		},
		// Unknown method - error
		{
			pb.Request{Method: "BADMETHOD", ID: 1},
			Response{Err: errors.ErrUnknownMethod},
			[]testutil.Action{},
		},
	}

	for i, tt := range tests {
		st := mockstore.NewRecorder()
		srv := &EtcdServer{
			lgMu:    new(sync.RWMutex),
			lg:      zaptest.NewLogger(t),
			v2store: st,
		}
		srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}
		resp := srv.applyV2Request((*RequestV2)(&tt.req), membership.ApplyBoth)

		if !reflect.DeepEqual(resp, tt.wresp) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, tt.wresp)
		}
		gaction := st.Action()
		if !reflect.DeepEqual(gaction, tt.wactions) {
			t.Errorf("#%d: action = %#v, want %#v", i, gaction, tt.wactions)
		}
	}
}

func TestApplyRequestOnAdminMemberAttributes(t *testing.T) {
	cl := newTestCluster(t, []*membership.Member{{ID: 1}})
	srv := &EtcdServer{
		lgMu:    new(sync.RWMutex),
		lg:      zaptest.NewLogger(t),
		v2store: mockstore.NewRecorder(),
		cluster: cl,
	}
	srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}

	req := pb.Request{
		Method: "PUT",
		ID:     1,
		Path:   membership.MemberAttributesStorePath(1),
		Val:    `{"Name":"abc","ClientURLs":["http://127.0.0.1:2379"]}`,
	}
	srv.applyV2Request((*RequestV2)(&req), membership.ApplyBoth)
	w := membership.Attributes{Name: "abc", ClientURLs: []string{"http://127.0.0.1:2379"}}
	if g := cl.Member(1).Attributes; !reflect.DeepEqual(g, w) {
		t.Errorf("attributes = %v, want %v", g, w)
	}
}

func TestApplyConfChangeError(t *testing.T) {
	cl := membership.NewCluster(zaptest.NewLogger(t))
	cl.SetStore(v2store.New())
	for i := 1; i <= 4; i++ {
		cl.AddMember(&membership.Member{ID: types.ID(i)}, true)
	}
	cl.RemoveMember(4, true)

	attr := membership.RaftAttributes{PeerURLs: []string{fmt.Sprintf("http://127.0.0.1:%d", 1)}}
	ctx, err := json.Marshal(&membership.Member{ID: types.ID(1), RaftAttributes: attr})
	if err != nil {
		t.Fatal(err)
	}

	attr = membership.RaftAttributes{PeerURLs: []string{fmt.Sprintf("http://127.0.0.1:%d", 4)}}
	ctx4, err := json.Marshal(&membership.Member{ID: types.ID(1), RaftAttributes: attr})
	if err != nil {
		t.Fatal(err)
	}

	attr = membership.RaftAttributes{PeerURLs: []string{fmt.Sprintf("http://127.0.0.1:%d", 5)}}
	ctx5, err := json.Marshal(&membership.Member{ID: types.ID(1), RaftAttributes: attr})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		cc   raftpb.ConfChange
		werr error
	}{
		{
			raftpb.ConfChange{
				Type:    raftpb.ConfChangeAddNode,
				NodeID:  4,
				Context: ctx4,
			},
			membership.ErrIDRemoved,
		},
		{
			raftpb.ConfChange{
				Type:    raftpb.ConfChangeUpdateNode,
				NodeID:  4,
				Context: ctx4,
			},
			membership.ErrIDRemoved,
		},
		{
			raftpb.ConfChange{
				Type:    raftpb.ConfChangeAddNode,
				NodeID:  1,
				Context: ctx,
			},
			membership.ErrIDExists,
		},
		{
			raftpb.ConfChange{
				Type:    raftpb.ConfChangeRemoveNode,
				NodeID:  5,
				Context: ctx5,
			},
			membership.ErrIDNotFound,
		},
	}
	for i, tt := range tests {
		n := newNodeRecorder()
		srv := &EtcdServer{
			lgMu:    new(sync.RWMutex),
			lg:      zaptest.NewLogger(t),
			r:       *newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), Node: n}),
			cluster: cl,
		}
		_, err := srv.applyConfChange(tt.cc, nil, true)
		if err != tt.werr {
			t.Errorf("#%d: applyConfChange error = %v, want %v", i, err, tt.werr)
		}
		cc := raftpb.ConfChange{Type: tt.cc.Type, NodeID: raft.None, Context: tt.cc.Context}
		w := []testutil.Action{
			{
				Name:   "ApplyConfChange",
				Params: []interface{}{cc},
			},
		}
		if g, _ := n.Wait(1); !reflect.DeepEqual(g, w) {
			t.Errorf("#%d: action = %+v, want %+v", i, g, w)
		}
	}
}

func TestApplyConfChangeShouldStop(t *testing.T) {
	cl := membership.NewCluster(zaptest.NewLogger(t))
	cl.SetStore(v2store.New())
	for i := 1; i <= 3; i++ {
		cl.AddMember(&membership.Member{ID: types.ID(i)}, true)
	}
	r := newRaftNode(raftNodeConfig{
		lg:        zaptest.NewLogger(t),
		Node:      newNodeNop(),
		transport: newNopTransporter(),
	})
	lg := zaptest.NewLogger(t)
	srv := &EtcdServer{
		lgMu:     new(sync.RWMutex),
		lg:       lg,
		memberId: 1,
		r:        *r,
		cluster:  cl,
		beHooks:  serverstorage.NewBackendHooks(lg, nil),
	}
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: 2,
	}
	// remove non-local member
	shouldStop, err := srv.applyConfChange(cc, &raftpb.ConfState{}, true)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if shouldStop {
		t.Errorf("shouldStop = %t, want %t", shouldStop, false)
	}

	// remove local member
	cc.NodeID = 1
	shouldStop, err = srv.applyConfChange(cc, &raftpb.ConfState{}, true)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if !shouldStop {
		t.Errorf("shouldStop = %t, want %t", shouldStop, true)
	}
}

// TestApplyConfigChangeUpdatesConsistIndex ensures a config change also updates the consistIndex
// where consistIndex equals to applied index.
func TestApplyConfigChangeUpdatesConsistIndex(t *testing.T) {
	lg := zaptest.NewLogger(t)

	cl := membership.NewCluster(zaptest.NewLogger(t))
	cl.SetStore(v2store.New())
	cl.AddMember(&membership.Member{ID: types.ID(1)}, true)

	be, _ := betesting.NewDefaultBoltTmpBackend(t)
	defer betesting.Close(t, be)
	schema.CreateMetaBucket(be.BatchTx())

	ci := cindex.NewConsistentIndex(be)
	srv := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		lg:           lg,
		memberId:     1,
		r:            *realisticRaftNode(lg),
		cluster:      cl,
		w:            wait.New(),
		consistIndex: ci,
		beHooks:      serverstorage.NewBackendHooks(lg, ci),
	}

	// create EntryConfChange entry
	now := time.Now()
	urls, err := types.NewURLs([]string{"http://whatever:123"})
	if err != nil {
		t.Fatal(err)
	}
	m := membership.NewMember("", urls, "", &now)
	m.ID = types.ID(2)
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	cc := &raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 2, Context: b}
	ents := []raftpb.Entry{{
		Index: 2,
		Term:  4,
		Type:  raftpb.EntryConfChange,
		Data:  pbutil.MustMarshal(cc),
	}}

	_, appliedi, _ := srv.apply(ents, &raftpb.ConfState{})
	consistIndex := srv.consistIndex.ConsistentIndex()
	assert.Equal(t, uint64(2), appliedi)

	t.Run("verify-backend", func(t *testing.T) {
		tx := be.BatchTx()
		tx.Lock()
		defer tx.Unlock()
		srv.beHooks.OnPreCommitUnsafe(tx)
		assert.Equal(t, raftpb.ConfState{Voters: []uint64{2}}, *schema.UnsafeConfStateFromBackend(lg, tx))
	})
	rindex, _ := schema.ReadConsistentIndex(be.ReadTx())
	assert.Equal(t, consistIndex, rindex)
}

func realisticRaftNode(lg *zap.Logger) *raftNode {
	storage := raft.NewMemoryStorage()
	storage.SetHardState(raftpb.HardState{Commit: 0, Term: 0})
	c := &raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
	}
	n := raft.RestartNode(c)
	r := newRaftNode(raftNodeConfig{
		lg:        lg,
		Node:      n,
		transport: newNopTransporter(),
	})
	return r
}

// TestApplyMultiConfChangeShouldStop ensures that toApply will return shouldStop
// if the local member is removed along with other conf updates.
func TestApplyMultiConfChangeShouldStop(t *testing.T) {
	lg := zaptest.NewLogger(t)
	cl := membership.NewCluster(lg)
	cl.SetStore(v2store.New())
	for i := 1; i <= 5; i++ {
		cl.AddMember(&membership.Member{ID: types.ID(i)}, true)
	}
	r := newRaftNode(raftNodeConfig{
		lg:        lg,
		Node:      newNodeNop(),
		transport: newNopTransporter(),
	})
	ci := cindex.NewFakeConsistentIndex(0)
	srv := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		lg:           lg,
		memberId:     2,
		r:            *r,
		cluster:      cl,
		w:            wait.New(),
		consistIndex: ci,
		beHooks:      serverstorage.NewBackendHooks(lg, ci),
	}
	var ents []raftpb.Entry
	for i := 1; i <= 4; i++ {
		ent := raftpb.Entry{
			Term:  1,
			Index: uint64(i),
			Type:  raftpb.EntryConfChange,
			Data: pbutil.MustMarshal(
				&raftpb.ConfChange{
					Type:   raftpb.ConfChangeRemoveNode,
					NodeID: uint64(i)}),
		}
		ents = append(ents, ent)
	}

	_, _, shouldStop := srv.apply(ents, &raftpb.ConfState{})
	if !shouldStop {
		t.Errorf("shouldStop = %t, want %t", shouldStop, true)
	}
}

func TestDoProposal(t *testing.T) {
	tests := []pb.Request{
		{Method: "POST", ID: 1},
		{Method: "PUT", ID: 1},
		{Method: "DELETE", ID: 1},
		{Method: "GET", ID: 1, Quorum: true},
	}
	for i, tt := range tests {
		st := mockstore.NewRecorder()
		r := newRaftNode(raftNodeConfig{
			lg:          zaptest.NewLogger(t),
			Node:        newNodeCommitter(),
			storage:     mockstorage.NewStorageRecorder(""),
			raftStorage: raft.NewMemoryStorage(),
			transport:   newNopTransporter(),
		})
		srv := &EtcdServer{
			lgMu:         new(sync.RWMutex),
			lg:           zaptest.NewLogger(t),
			Cfg:          config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 1, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries},
			r:            *r,
			v2store:      st,
			reqIDGen:     idutil.NewGenerator(0, time.Time{}),
			SyncTicker:   &time.Ticker{},
			consistIndex: cindex.NewFakeConsistentIndex(0),
		}
		srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}
		srv.start()
		resp, err := srv.Do(context.Background(), tt)
		srv.Stop()

		action := st.Action()
		if len(action) != 1 {
			t.Errorf("#%d: len(action) = %d, want 1", i, len(action))
		}
		if err != nil {
			t.Fatalf("#%d: err = %v, want nil", i, err)
		}
		// resp.Index is set in Do() based on the raft state; may either be 0 or 1
		wresp := Response{Event: &v2store.Event{}, Index: resp.Index}
		if !reflect.DeepEqual(resp, wresp) {
			t.Errorf("#%d: resp = %v, want %v", i, resp, wresp)
		}
	}
}

func TestDoProposalCancelled(t *testing.T) {
	wt := mockwait.NewRecorder()
	srv := &EtcdServer{
		lgMu:     new(sync.RWMutex),
		lg:       zaptest.NewLogger(t),
		Cfg:      config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 1, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries},
		r:        *newRaftNode(raftNodeConfig{Node: newNodeNop()}),
		w:        wt,
		reqIDGen: idutil.NewGenerator(0, time.Time{}),
	}
	srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := srv.Do(ctx, pb.Request{Method: "PUT"})

	if err != errors.ErrCanceled {
		t.Fatalf("err = %v, want %v", err, errors.ErrCanceled)
	}
	w := []testutil.Action{{Name: "Register"}, {Name: "Trigger"}}
	if !reflect.DeepEqual(wt.Action(), w) {
		t.Errorf("wt.action = %+v, want %+v", wt.Action(), w)
	}
}

func TestDoProposalTimeout(t *testing.T) {
	srv := &EtcdServer{
		lgMu:     new(sync.RWMutex),
		lg:       zaptest.NewLogger(t),
		Cfg:      config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 1, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries},
		r:        *newRaftNode(raftNodeConfig{Node: newNodeNop()}),
		w:        mockwait.NewNop(),
		reqIDGen: idutil.NewGenerator(0, time.Time{}),
	}
	srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	_, err := srv.Do(ctx, pb.Request{Method: "PUT"})
	cancel()
	if err != errors.ErrTimeout {
		t.Fatalf("err = %v, want %v", err, errors.ErrTimeout)
	}
}

func TestDoProposalStopped(t *testing.T) {
	srv := &EtcdServer{
		lgMu:     new(sync.RWMutex),
		lg:       zaptest.NewLogger(t),
		Cfg:      config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 1, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries},
		r:        *newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), Node: newNodeNop()}),
		w:        mockwait.NewNop(),
		reqIDGen: idutil.NewGenerator(0, time.Time{}),
	}
	srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}

	srv.stopping = make(chan struct{})
	close(srv.stopping)
	_, err := srv.Do(context.Background(), pb.Request{Method: "PUT", ID: 1})
	if err != errors.ErrStopped {
		t.Errorf("err = %v, want %v", err, errors.ErrStopped)
	}
}

// TestSync tests sync 1. is nonblocking 2. proposes SYNC request.
func TestSync(t *testing.T) {
	n := newNodeRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	srv := &EtcdServer{
		lgMu:     new(sync.RWMutex),
		lg:       zaptest.NewLogger(t),
		r:        *newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), Node: n}),
		reqIDGen: idutil.NewGenerator(0, time.Time{}),
		ctx:      ctx,
		cancel:   cancel,
	}
	srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}

	// check that sync is non-blocking
	done := make(chan struct{}, 1)
	go func() {
		srv.sync(10 * time.Second)
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sync should be non-blocking but did not return after 1s!")
	}

	action, _ := n.Wait(1)
	if len(action) != 1 {
		t.Fatalf("len(action) = %d, want 1", len(action))
	}
	if action[0].Name != "Propose" {
		t.Fatalf("action = %s, want Propose", action[0].Name)
	}
	data := action[0].Params[0].([]byte)
	var r pb.Request
	if err := r.Unmarshal(data); err != nil {
		t.Fatalf("unmarshal request error: %v", err)
	}
	if r.Method != "SYNC" {
		t.Errorf("method = %s, want SYNC", r.Method)
	}
}

// TestSyncTimeout tests the case that sync 1. is non-blocking 2. cancel request
// after timeout
func TestSyncTimeout(t *testing.T) {
	n := newProposalBlockerRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	srv := &EtcdServer{
		lgMu:     new(sync.RWMutex),
		lg:       zaptest.NewLogger(t),
		r:        *newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), Node: n}),
		reqIDGen: idutil.NewGenerator(0, time.Time{}),
		ctx:      ctx,
		cancel:   cancel,
	}
	srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}

	// check that sync is non-blocking
	done := make(chan struct{}, 1)
	go func() {
		srv.sync(0)
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sync should be non-blocking but did not return after 1s!")
	}

	w := []testutil.Action{{Name: "Propose blocked"}}
	if g, _ := n.Wait(1); !reflect.DeepEqual(g, w) {
		t.Errorf("action = %v, want %v", g, w)
	}
}

// TODO: TestNoSyncWhenNoLeader

// TestSyncTrigger tests that the server proposes a SYNC request when its sync timer ticks
func TestSyncTrigger(t *testing.T) {
	n := newReadyNode()
	st := make(chan time.Time, 1)
	tk := &time.Ticker{C: st}
	r := newRaftNode(raftNodeConfig{
		lg:          zaptest.NewLogger(t),
		Node:        n,
		raftStorage: raft.NewMemoryStorage(),
		transport:   newNopTransporter(),
		storage:     mockstorage.NewStorageRecorder(""),
	})

	srv := &EtcdServer{
		lgMu:       new(sync.RWMutex),
		lg:         zaptest.NewLogger(t),
		Cfg:        config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 1, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries},
		r:          *r,
		v2store:    mockstore.NewNop(),
		SyncTicker: tk,
		reqIDGen:   idutil.NewGenerator(0, time.Time{}),
	}

	// trigger the server to become a leader and accept sync requests
	go func() {
		srv.start()
		n.readyc <- raft.Ready{
			SoftState: &raft.SoftState{
				RaftState: raft.StateLeader,
			},
		}
		// trigger a sync request
		st <- time.Time{}
	}()

	action, _ := n.Wait(1)
	go srv.Stop()

	if len(action) != 1 {
		t.Fatalf("len(action) = %d, want 1", len(action))
	}
	if action[0].Name != "Propose" {
		t.Fatalf("action = %s, want Propose", action[0].Name)
	}
	data := action[0].Params[0].([]byte)
	var req pb.Request
	if err := req.Unmarshal(data); err != nil {
		t.Fatalf("error unmarshalling data: %v", err)
	}
	if req.Method != "SYNC" {
		t.Fatalf("unexpected proposed request: %#v", req.Method)
	}

	// wait on stop message
	<-n.Chan()
}

// TestSnapshot should snapshot the store and cut the persistent
func TestSnapshot(t *testing.T) {
	be, _ := betesting.NewDefaultBadgerTmpBackend(t)

	s := raft.NewMemoryStorage()
	s.Append([]raftpb.Entry{{Index: 1}})
	st := mockstore.NewRecorderStream()
	p := mockstorage.NewStorageRecorderStream("")
	r := newRaftNode(raftNodeConfig{
		lg:          zaptest.NewLogger(t),
		Node:        newNodeNop(),
		raftStorage: s,
		storage:     p,
	})
	srv := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		lg:           zaptest.NewLogger(t),
		r:            *r,
		v2store:      st,
		consistIndex: cindex.NewConsistentIndex(be),
	}
	srv.kv = mvcc.New(zaptest.NewLogger(t), be, &lease.FakeLessor{}, mvcc.StoreConfig{})
	srv.be = be

	ch := make(chan struct{}, 2)

	go func() {
		gaction, _ := p.Wait(2)
		defer func() { ch <- struct{}{} }()

		if len(gaction) != 2 {
			t.Errorf("len(action) = %d, want 2", len(gaction))
			return
		}
		if !reflect.DeepEqual(gaction[0], testutil.Action{Name: "SaveSnap"}) {
			t.Errorf("action = %s, want SaveSnap", gaction[0])
		}

		if !reflect.DeepEqual(gaction[1], testutil.Action{Name: "Release"}) {
			t.Errorf("action = %s, want Release", gaction[1])
		}
	}()

	go func() {
		gaction, _ := st.Wait(2)
		defer func() { ch <- struct{}{} }()

		if len(gaction) != 2 {
			t.Errorf("len(action) = %d, want 2", len(gaction))
		}
		if !reflect.DeepEqual(gaction[0], testutil.Action{Name: "Clone"}) {
			t.Errorf("action = %s, want Clone", gaction[0])
		}
		if !reflect.DeepEqual(gaction[1], testutil.Action{Name: "SaveNoCopy"}) {
			t.Errorf("action = %s, want SaveNoCopy", gaction[1])
		}
	}()

	srv.snapshot(1, raftpb.ConfState{Voters: []uint64{1}})
	<-ch
	<-ch
}

// TestSnapshotOrdering ensures raft persists snapshot onto disk before
// snapshot db is applied.
func TestSnapshotOrdering(t *testing.T) {
	// Ignore the snapshot index verification in unit test, because
	// it doesn't follow the e2e applying logic.
	revertFunc := verify.DisableVerifications()
	defer revertFunc()

	lg := zaptest.NewLogger(t)
	n := newNopReadyNode()
	st := v2store.New()
	cl := membership.NewCluster(lg)
	cl.SetStore(st)

	testdir := t.TempDir()

	snapdir := filepath.Join(testdir, "member", "snap")
	if err := os.MkdirAll(snapdir, 0755); err != nil {
		t.Fatalf("couldn't make snap dir (%v)", err)
	}

	rs := raft.NewMemoryStorage()
	p := mockstorage.NewStorageRecorderStream(testdir)
	tr, snapDoneC := newSnapTransporter(lg, snapdir)
	r := newRaftNode(raftNodeConfig{
		lg:          lg,
		isIDRemoved: func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
		Node:        n,
		transport:   tr,
		storage:     p,
		raftStorage: rs,
	})
	be, _ := betesting.NewDefaultBoltTmpBackend(t)
	ci := cindex.NewConsistentIndex(be)
	s := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		lg:           lg,
		Cfg:          config.ServerConfig{Logger: lg, DataDir: testdir, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries},
		r:            *r,
		v2store:      st,
		snapshotter:  snap.New(lg, snapdir),
		cluster:      cl,
		SyncTicker:   &time.Ticker{},
		consistIndex: ci,
		beHooks:      serverstorage.NewBackendHooks(lg, ci),
	}
	s.applyV2 = &applierV2store{store: s.v2store, cluster: s.cluster}

	s.kv = mvcc.New(lg, be, &lease.FakeLessor{}, mvcc.StoreConfig{})
	s.be = be

	s.start()
	defer s.Stop()

	n.readyc <- raft.Ready{Messages: []raftpb.Message{{Type: raftpb.MsgSnap}}}
	go func() {
		// get the snapshot sent by the transport
		snapMsg := <-snapDoneC
		// Snapshot first triggers raftnode to persists the snapshot onto disk
		// before renaming db snapshot file to db
		snapMsg.Snapshot.Metadata.Index = 1
		n.readyc <- raft.Ready{Snapshot: *snapMsg.Snapshot}
	}()

	ac := <-p.Chan()
	if ac.Name != "Save" {
		t.Fatalf("expected Save, got %+v", ac)
	}

	if ac := <-p.Chan(); ac.Name != "SaveSnap" {
		t.Fatalf("expected SaveSnap, got %+v", ac)
	}

	if ac := <-p.Chan(); ac.Name != "Save" {
		t.Fatalf("expected Save, got %+v", ac)
	}

	// confirm snapshot file still present before calling SaveSnap
	snapPath := filepath.Join(snapdir, fmt.Sprintf("%016x.snap.db", 1))
	if !fileutil.Exist(snapPath) {
		t.Fatalf("expected file %q, got missing", snapPath)
	}

	// unblock SaveSnapshot, etcdserver now permitted to move snapshot file
	if ac := <-p.Chan(); ac.Name != "Sync" {
		t.Fatalf("expected Sync, got %+v", ac)
	}

	if ac := <-p.Chan(); ac.Name != "Release" {
		t.Fatalf("expected Release, got %+v", ac)
	}
}

// TestTriggerSnap for Applied > SnapshotCount should trigger a SaveSnap event
func TestTriggerSnap(t *testing.T) {
	be, tmpPath := betesting.NewDefaultBadgerTmpBackend(t)
	defer func() {
		os.RemoveAll(tmpPath)
	}()

	snapc := 10
	st := mockstore.NewRecorder()
	p := mockstorage.NewStorageRecorderStream("")
	r := newRaftNode(raftNodeConfig{
		lg:          zaptest.NewLogger(t),
		Node:        newNodeCommitter(),
		raftStorage: raft.NewMemoryStorage(),
		storage:     p,
		transport:   newNopTransporter(),
	})
	srv := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		lg:           zaptest.NewLogger(t),
		Cfg:          config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 1, SnapshotCount: uint64(snapc), SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries},
		r:            *r,
		v2store:      st,
		reqIDGen:     idutil.NewGenerator(0, time.Time{}),
		SyncTicker:   &time.Ticker{},
		consistIndex: cindex.NewConsistentIndex(be),
	}
	srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}

	srv.kv = mvcc.New(zaptest.NewLogger(t), be, &lease.FakeLessor{}, mvcc.StoreConfig{})
	srv.be = be

	srv.start()

	donec := make(chan struct{})
	go func() {
		defer close(donec)
		wcnt := 3 + snapc
		gaction, _ := p.Wait(wcnt)

		// each operation is recorded as a Save
		// (SnapshotCount+1) * Puts + SaveSnap = (SnapshotCount+1) * Save + SaveSnap + Release
		if len(gaction) != wcnt {
			t.Logf("gaction: %v", gaction)
			t.Errorf("len(action) = %d, want %d", len(gaction), wcnt)
			return
		}
		if !reflect.DeepEqual(gaction[wcnt-2], testutil.Action{Name: "SaveSnap"}) {
			t.Errorf("action = %s, want SaveSnap", gaction[wcnt-2])
		}

		if !reflect.DeepEqual(gaction[wcnt-1], testutil.Action{Name: "Release"}) {
			t.Errorf("action = %s, want Release", gaction[wcnt-1])
		}
	}()

	for i := 0; i < snapc+1; i++ {
		srv.Do(context.Background(), pb.Request{Method: "PUT"})
	}

	<-donec
	srv.Stop()
}

// TestConcurrentApplyAndSnapshotV3 will send out snapshots concurrently with
// proposals.
func TestConcurrentApplyAndSnapshotV3(t *testing.T) {
	// Ignore the snapshot index verification in unit test, because
	// it doesn't follow the e2e applying logic.
	revertFunc := verify.DisableVerifications()
	defer revertFunc()

	lg := zaptest.NewLogger(t)
	n := newNopReadyNode()
	st := v2store.New()
	cl := membership.NewCluster(lg)
	cl.SetStore(st)

	testdir := t.TempDir()
	if err := os.MkdirAll(testdir+"/member/snap", 0755); err != nil {
		t.Fatalf("Couldn't make snap dir (%v)", err)
	}

	rs := raft.NewMemoryStorage()
	tr, snapDoneC := newSnapTransporter(lg, testdir)
	r := newRaftNode(raftNodeConfig{
		lg:          lg,
		isIDRemoved: func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
		Node:        n,
		transport:   tr,
		storage:     mockstorage.NewStorageRecorder(testdir),
		raftStorage: rs,
	})
	be, _ := betesting.NewDefaultSqliteTmpBackend(t)
	ci := cindex.NewConsistentIndex(be)
	s := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		dbType:       "sqlite",
		lg:           lg,
		Cfg:          config.ServerConfig{Logger: lg, DataDir: testdir, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries, DBType: backend.SQLite},
		r:            *r,
		v2store:      st,
		snapshotter:  snap.New(lg, testdir),
		cluster:      cl,
		SyncTicker:   &time.Ticker{},
		consistIndex: ci,
		beHooks:      serverstorage.NewBackendHooks(lg, ci),
	}
	s.applyV2 = &applierV2store{store: s.v2store, cluster: s.cluster}

	s.kv = mvcc.New(lg, be, &lease.FakeLessor{}, mvcc.StoreConfig{})
	s.be = be

	s.start()
	defer s.Stop()

	// submit applied entries and snap entries
	idx := uint64(0)
	outdated := 0
	accepted := 0
	for k := 1; k <= 101; k++ {
		idx++
		ch := s.w.Register(idx)
		req := &pb.Request{Method: "QGET", ID: idx}
		ent := raftpb.Entry{Index: idx, Data: pbutil.MustMarshal(req)}
		ready := raft.Ready{Entries: []raftpb.Entry{ent}}
		n.readyc <- ready

		ready = raft.Ready{CommittedEntries: []raftpb.Entry{ent}}
		n.readyc <- ready

		// "idx" applied
		<-ch

		// one snapshot for every two messages
		if k%2 != 0 {
			continue
		}

		n.readyc <- raft.Ready{Messages: []raftpb.Message{{Type: raftpb.MsgSnap}}}
		// get the snapshot sent by the transport
		snapMsg := <-snapDoneC
		// If the snapshot trails applied records, recovery will panic
		// since there's no allocated snapshot at the place of the
		// snapshot record. This only happens when the applier and the
		// snapshot sender get out of sync.
		if snapMsg.Snapshot.Metadata.Index == idx {
			idx++
			snapMsg.Snapshot.Metadata.Index = idx
			ready = raft.Ready{Snapshot: *snapMsg.Snapshot}
			n.readyc <- ready
			accepted++
		} else {
			outdated++
		}
		// don't wait for the snapshot to complete, move to next message
	}
	if accepted != 50 {
		t.Errorf("accepted=%v, want 50", accepted)
	}
	if outdated != 0 {
		t.Errorf("outdated=%v, want 0", outdated)
	}
}

// TestAddMember tests AddMember can propose and perform node addition.
func TestAddMember(t *testing.T) {
	lg := zaptest.NewLogger(t)
	n := newNodeConfChangeCommitterRecorder()
	n.readyc <- raft.Ready{
		SoftState: &raft.SoftState{RaftState: raft.StateLeader},
	}
	cl := newTestCluster(t, nil)
	st := v2store.New()
	cl.SetStore(st)
	r := newRaftNode(raftNodeConfig{
		lg:          lg,
		Node:        n,
		raftStorage: raft.NewMemoryStorage(),
		storage:     mockstorage.NewStorageRecorder(""),
		transport:   newNopTransporter(),
	})
	s := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		lg:           lg,
		r:            *r,
		v2store:      st,
		cluster:      cl,
		reqIDGen:     idutil.NewGenerator(0, time.Time{}),
		SyncTicker:   &time.Ticker{},
		consistIndex: cindex.NewFakeConsistentIndex(0),
		beHooks:      serverstorage.NewBackendHooks(lg, nil),
	}
	s.start()
	m := membership.Member{ID: 1234, RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"foo"}}}
	_, err := s.AddMember(context.Background(), m)
	gaction := n.Action()
	s.Stop()

	if err != nil {
		t.Fatalf("AddMember error: %v", err)
	}
	wactions := []testutil.Action{{Name: "ProposeConfChange:ConfChangeAddNode"}, {Name: "ApplyConfChange:ConfChangeAddNode"}}
	if !reflect.DeepEqual(gaction, wactions) {
		t.Errorf("action = %v, want %v", gaction, wactions)
	}
	if cl.Member(1234) == nil {
		t.Errorf("member with id 1234 is not added")
	}
}

// TestRemoveMember tests RemoveMember can propose and perform node removal.
func TestRemoveMember(t *testing.T) {
	lg := zaptest.NewLogger(t)
	n := newNodeConfChangeCommitterRecorder()
	n.readyc <- raft.Ready{
		SoftState: &raft.SoftState{RaftState: raft.StateLeader},
	}
	cl := newTestCluster(t, nil)
	st := v2store.New()
	cl.SetStore(v2store.New())
	cl.AddMember(&membership.Member{ID: 1234}, true)
	r := newRaftNode(raftNodeConfig{
		lg:          lg,
		Node:        n,
		raftStorage: raft.NewMemoryStorage(),
		storage:     mockstorage.NewStorageRecorder(""),
		transport:   newNopTransporter(),
	})
	s := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		lg:           zaptest.NewLogger(t),
		r:            *r,
		v2store:      st,
		cluster:      cl,
		reqIDGen:     idutil.NewGenerator(0, time.Time{}),
		SyncTicker:   &time.Ticker{},
		consistIndex: cindex.NewFakeConsistentIndex(0),
		beHooks:      serverstorage.NewBackendHooks(lg, nil),
	}
	s.start()
	_, err := s.RemoveMember(context.Background(), 1234)
	gaction := n.Action()
	s.Stop()

	if err != nil {
		t.Fatalf("RemoveMember error: %v", err)
	}
	wactions := []testutil.Action{{Name: "ProposeConfChange:ConfChangeRemoveNode"}, {Name: "ApplyConfChange:ConfChangeRemoveNode"}}
	if !reflect.DeepEqual(gaction, wactions) {
		t.Errorf("action = %v, want %v", gaction, wactions)
	}
	if cl.Member(1234) != nil {
		t.Errorf("member with id 1234 is not removed")
	}
}

// TestUpdateMember tests RemoveMember can propose and perform node update.
func TestUpdateMember(t *testing.T) {
	lg := zaptest.NewLogger(t)
	n := newNodeConfChangeCommitterRecorder()
	n.readyc <- raft.Ready{
		SoftState: &raft.SoftState{RaftState: raft.StateLeader},
	}
	cl := newTestCluster(t, nil)
	st := v2store.New()
	cl.SetStore(st)
	cl.AddMember(&membership.Member{ID: 1234}, true)
	r := newRaftNode(raftNodeConfig{
		lg:          lg,
		Node:        n,
		raftStorage: raft.NewMemoryStorage(),
		storage:     mockstorage.NewStorageRecorder(""),
		transport:   newNopTransporter(),
	})
	s := &EtcdServer{
		lgMu:         new(sync.RWMutex),
		lg:           lg,
		r:            *r,
		v2store:      st,
		cluster:      cl,
		reqIDGen:     idutil.NewGenerator(0, time.Time{}),
		SyncTicker:   &time.Ticker{},
		consistIndex: cindex.NewFakeConsistentIndex(0),
		beHooks:      serverstorage.NewBackendHooks(lg, nil),
	}
	s.start()
	wm := membership.Member{ID: 1234, RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://127.0.0.1:1"}}}
	_, err := s.UpdateMember(context.Background(), wm)
	gaction := n.Action()
	s.Stop()

	if err != nil {
		t.Fatalf("UpdateMember error: %v", err)
	}
	wactions := []testutil.Action{{Name: "ProposeConfChange:ConfChangeUpdateNode"}, {Name: "ApplyConfChange:ConfChangeUpdateNode"}}
	if !reflect.DeepEqual(gaction, wactions) {
		t.Errorf("action = %v, want %v", gaction, wactions)
	}
	if !reflect.DeepEqual(cl.Member(1234), &wm) {
		t.Errorf("member = %v, want %v", cl.Member(1234), &wm)
	}
}

// TODO: test server could stop itself when being removed

func TestPublishV3(t *testing.T) {
	n := newNodeRecorder()
	ch := make(chan interface{}, 1)
	// simulate that request has gone through consensus
	ch <- &apply2.Result{}
	w := wait.NewWithResponse(ch)
	ctx, cancel := context.WithCancel(context.Background())
	lg := zaptest.NewLogger(t)
	be, _ := betesting.NewDefaultBoltTmpBackend(t)
	srv := &EtcdServer{
		lgMu:       new(sync.RWMutex),
		lg:         lg,
		readych:    make(chan struct{}),
		Cfg:        config.ServerConfig{Logger: lg, TickMs: 1, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries, MaxRequestBytes: 1000},
		memberId:   1,
		r:          *newRaftNode(raftNodeConfig{lg: lg, Node: n}),
		attributes: membership.Attributes{Name: "node1", ClientURLs: []string{"http://a", "http://b"}},
		cluster:    &membership.RaftCluster{},
		w:          w,
		reqIDGen:   idutil.NewGenerator(0, time.Time{}),
		SyncTicker: &time.Ticker{},
		authStore:  auth.NewAuthStore(lg, schema.NewAuthBackend(lg, be), nil, 0),
		be:         be,
		ctx:        ctx,
		cancel:     cancel,
	}
	srv.publishV3(time.Hour)

	action := n.Action()
	if len(action) != 1 {
		t.Fatalf("len(action) = %d, want 1", len(action))
	}
	if action[0].Name != "Propose" {
		t.Fatalf("action = %s, want Propose", action[0].Name)
	}
	data := action[0].Params[0].([]byte)
	var r pb.InternalRaftRequest
	if err := r.Unmarshal(data); err != nil {
		t.Fatalf("unmarshal request error: %v", err)
	}
	assert.Equal(t, &membershippb.ClusterMemberAttrSetRequest{Member_ID: 0x1, MemberAttributes: &membershippb.Attributes{
		Name: "node1", ClientUrls: []string{"http://a", "http://b"}}}, r.ClusterMemberAttrSet)
}

// TestPublishV3Stopped tests that publish will be stopped if server is stopped.
func TestPublishV3Stopped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := newRaftNode(raftNodeConfig{
		lg:        zaptest.NewLogger(t),
		Node:      newNodeNop(),
		transport: newNopTransporter(),
	})
	srv := &EtcdServer{
		lgMu:       new(sync.RWMutex),
		lg:         zaptest.NewLogger(t),
		Cfg:        config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 1, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries},
		r:          *r,
		cluster:    &membership.RaftCluster{},
		w:          mockwait.NewNop(),
		done:       make(chan struct{}),
		stopping:   make(chan struct{}),
		stop:       make(chan struct{}),
		reqIDGen:   idutil.NewGenerator(0, time.Time{}),
		SyncTicker: &time.Ticker{},

		ctx:    ctx,
		cancel: cancel,
	}
	close(srv.stopping)
	srv.publishV3(time.Hour)
}

// TestPublishV3Retry tests that publish will keep retry until success.
func TestPublishV3Retry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n := newNodeRecorderStream()

	lg := zaptest.NewLogger(t)
	be, _ := betesting.NewDefaultBoltTmpBackend(t)
	srv := &EtcdServer{
		lgMu:       new(sync.RWMutex),
		lg:         lg,
		readych:    make(chan struct{}),
		Cfg:        config.ServerConfig{Logger: lg, TickMs: 1, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries, MaxRequestBytes: 1000},
		memberId:   1,
		r:          *newRaftNode(raftNodeConfig{lg: lg, Node: n}),
		w:          mockwait.NewNop(),
		stopping:   make(chan struct{}),
		attributes: membership.Attributes{Name: "node1", ClientURLs: []string{"http://a", "http://b"}},
		cluster:    &membership.RaftCluster{},
		reqIDGen:   idutil.NewGenerator(0, time.Time{}),
		SyncTicker: &time.Ticker{},
		authStore:  auth.NewAuthStore(lg, schema.NewAuthBackend(lg, be), nil, 0),
		be:         be,
		ctx:        ctx,
		cancel:     cancel,
	}

	// expect multiple proposals from retrying
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		if action, err := n.Wait(2); err != nil {
			t.Errorf("len(action) = %d, want >= 2 (%v)", len(action), err)
		}
		close(srv.stopping)
		// drain remaining actions, if any, so publish can terminate
		for {
			select {
			case <-ch:
				return
			default:
				n.Action()
			}
		}
	}()
	srv.publishV3(10 * time.Nanosecond)
	ch <- struct{}{}
	<-ch
}

func TestUpdateVersion(t *testing.T) {
	n := newNodeRecorder()
	ch := make(chan interface{}, 1)
	// simulate that request has gone through consensus
	ch <- Response{}
	w := wait.NewWithResponse(ch)
	ctx, cancel := context.WithCancel(context.TODO())
	srv := &EtcdServer{
		lgMu:       new(sync.RWMutex),
		lg:         zaptest.NewLogger(t),
		memberId:   1,
		Cfg:        config.ServerConfig{Logger: zaptest.NewLogger(t), TickMs: 1, SnapshotCatchUpEntries: DefaultSnapshotCatchUpEntries},
		r:          *newRaftNode(raftNodeConfig{lg: zaptest.NewLogger(t), Node: n}),
		attributes: membership.Attributes{Name: "node1", ClientURLs: []string{"http://node1.com"}},
		cluster:    &membership.RaftCluster{},
		w:          w,
		reqIDGen:   idutil.NewGenerator(0, time.Time{}),
		SyncTicker: &time.Ticker{},

		ctx:    ctx,
		cancel: cancel,
	}
	srv.updateClusterVersionV2("2.0.0")

	action := n.Action()
	if len(action) != 1 {
		t.Fatalf("len(action) = %d, want 1", len(action))
	}
	if action[0].Name != "Propose" {
		t.Fatalf("action = %s, want Propose", action[0].Name)
	}
	data := action[0].Params[0].([]byte)
	var r pb.Request
	if err := r.Unmarshal(data); err != nil {
		t.Fatalf("unmarshal request error: %v", err)
	}
	if r.Method != "PUT" {
		t.Errorf("method = %s, want PUT", r.Method)
	}
	if wpath := path.Join(StoreClusterPrefix, "version"); r.Path != wpath {
		t.Errorf("path = %s, want %s", r.Path, wpath)
	}
	if r.Val != "2.0.0" {
		t.Errorf("val = %s, want %s", r.Val, "2.0.0")
	}
}

func TestStopNotify(t *testing.T) {
	s := &EtcdServer{
		lgMu: new(sync.RWMutex),
		lg:   zaptest.NewLogger(t),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		<-s.stop
		close(s.done)
	}()

	notifier := s.StopNotify()
	select {
	case <-notifier:
		t.Fatalf("received unexpected stop notification")
	default:
	}
	s.Stop()
	select {
	case <-notifier:
	default:
		t.Fatalf("cannot receive stop notification")
	}
}

func TestGetOtherPeerURLs(t *testing.T) {
	lg := zaptest.NewLogger(t)
	tests := []struct {
		membs []*membership.Member
		wurls []string
	}{
		{
			[]*membership.Member{
				membership.NewMember("1", types.MustNewURLs([]string{"http://10.0.0.1:1"}), "a", nil),
			},
			[]string{},
		},
		{
			[]*membership.Member{
				membership.NewMember("1", types.MustNewURLs([]string{"http://10.0.0.1:1"}), "a", nil),
				membership.NewMember("2", types.MustNewURLs([]string{"http://10.0.0.2:2"}), "a", nil),
				membership.NewMember("3", types.MustNewURLs([]string{"http://10.0.0.3:3"}), "a", nil),
			},
			[]string{"http://10.0.0.2:2", "http://10.0.0.3:3"},
		},
		{
			[]*membership.Member{
				membership.NewMember("1", types.MustNewURLs([]string{"http://10.0.0.1:1"}), "a", nil),
				membership.NewMember("3", types.MustNewURLs([]string{"http://10.0.0.3:3"}), "a", nil),
				membership.NewMember("2", types.MustNewURLs([]string{"http://10.0.0.2:2"}), "a", nil),
			},
			[]string{"http://10.0.0.2:2", "http://10.0.0.3:3"},
		},
	}
	for i, tt := range tests {
		cl := membership.NewClusterFromMembers(lg, types.ID(0), tt.membs)
		self := "1"
		urls := getRemotePeerURLs(cl, self)
		if !reflect.DeepEqual(urls, tt.wurls) {
			t.Errorf("#%d: urls = %+v, want %+v", i, urls, tt.wurls)
		}
	}
}

type nodeRecorder struct{ testutil.Recorder }

func newNodeRecorder() *nodeRecorder       { return &nodeRecorder{&testutil.RecorderBuffered{}} }
func newNodeRecorderStream() *nodeRecorder { return &nodeRecorder{testutil.NewRecorderStream()} }
func newNodeNop() raft.Node                { return newNodeRecorder() }

func (n *nodeRecorder) Tick() { n.Record(testutil.Action{Name: "Tick"}) }
func (n *nodeRecorder) Campaign(ctx context.Context) error {
	n.Record(testutil.Action{Name: "Campaign"})
	return nil
}
func (n *nodeRecorder) Propose(ctx context.Context, data []byte) error {
	n.Record(testutil.Action{Name: "Propose", Params: []interface{}{data}})
	return nil
}
func (n *nodeRecorder) ProposeConfChange(ctx context.Context, conf raftpb.ConfChangeI) error {
	n.Record(testutil.Action{Name: "ProposeConfChange"})
	return nil
}
func (n *nodeRecorder) Step(ctx context.Context, msg raftpb.Message) error {
	n.Record(testutil.Action{Name: "Step"})
	return nil
}
func (n *nodeRecorder) Status() raft.Status                                             { return raft.Status{} }
func (n *nodeRecorder) Ready() <-chan raft.Ready                                        { return nil }
func (n *nodeRecorder) TransferLeadership(ctx context.Context, lead, transferee uint64) {}
func (n *nodeRecorder) ReadIndex(ctx context.Context, rctx []byte) error                { return nil }
func (n *nodeRecorder) Advance()                                                        {}
func (n *nodeRecorder) ApplyConfChange(conf raftpb.ConfChangeI) *raftpb.ConfState {
	n.Record(testutil.Action{Name: "ApplyConfChange", Params: []interface{}{conf}})
	return &raftpb.ConfState{}
}

func (n *nodeRecorder) Stop() {
	n.Record(testutil.Action{Name: "Stop"})
}

func (n *nodeRecorder) ReportUnreachable(id uint64) {}

func (n *nodeRecorder) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func (n *nodeRecorder) Compact(index uint64, nodes []uint64, d []byte) {
	n.Record(testutil.Action{Name: "Compact"})
}

type nodeProposalBlockerRecorder struct {
	nodeRecorder
}

func newProposalBlockerRecorder() *nodeProposalBlockerRecorder {
	return &nodeProposalBlockerRecorder{*newNodeRecorderStream()}
}

func (n *nodeProposalBlockerRecorder) Propose(ctx context.Context, data []byte) error {
	<-ctx.Done()
	n.Record(testutil.Action{Name: "Propose blocked"})
	return nil
}

// readyNode is a nodeRecorder with a user-writeable ready channel
type readyNode struct {
	nodeRecorder
	readyc chan raft.Ready
}

func newReadyNode() *readyNode {
	return &readyNode{
		nodeRecorder{testutil.NewRecorderStream()},
		make(chan raft.Ready, 1)}
}
func newNopReadyNode() *readyNode {
	return &readyNode{*newNodeRecorder(), make(chan raft.Ready, 1)}
}

func (n *readyNode) Ready() <-chan raft.Ready { return n.readyc }

type nodeConfChangeCommitterRecorder struct {
	readyNode
	index uint64
}

func newNodeConfChangeCommitterRecorder() *nodeConfChangeCommitterRecorder {
	return &nodeConfChangeCommitterRecorder{*newNopReadyNode(), 0}
}

func newNodeConfChangeCommitterStream() *nodeConfChangeCommitterRecorder {
	return &nodeConfChangeCommitterRecorder{*newReadyNode(), 0}
}

func confChangeActionName(conf raftpb.ConfChangeI) string {
	var s string
	if confV1, ok := conf.AsV1(); ok {
		s = confV1.Type.String()
	} else {
		for i, chg := range conf.AsV2().Changes {
			if i > 0 {
				s += "/"
			}
			s += chg.Type.String()
		}
	}
	return s
}

func (n *nodeConfChangeCommitterRecorder) ProposeConfChange(ctx context.Context, conf raftpb.ConfChangeI) error {
	typ, data, err := raftpb.MarshalConfChange(conf)
	if err != nil {
		return err
	}

	n.index++
	n.Record(testutil.Action{Name: "ProposeConfChange:" + confChangeActionName(conf)})
	n.readyc <- raft.Ready{CommittedEntries: []raftpb.Entry{{Index: n.index, Type: typ, Data: data}}}
	return nil
}
func (n *nodeConfChangeCommitterRecorder) Ready() <-chan raft.Ready {
	return n.readyc
}
func (n *nodeConfChangeCommitterRecorder) ApplyConfChange(conf raftpb.ConfChangeI) *raftpb.ConfState {
	n.Record(testutil.Action{Name: "ApplyConfChange:" + confChangeActionName(conf)})
	return &raftpb.ConfState{}
}

// nodeCommitter commits proposed data immediately.
type nodeCommitter struct {
	readyNode
	index uint64
}

func newNodeCommitter() raft.Node {
	return &nodeCommitter{*newNopReadyNode(), 0}
}
func (n *nodeCommitter) Propose(ctx context.Context, data []byte) error {
	n.index++
	ents := []raftpb.Entry{{Index: n.index, Data: data}}
	n.readyc <- raft.Ready{
		Entries:          ents,
		CommittedEntries: ents,
	}
	return nil
}

func newTestCluster(t testing.TB, membs []*membership.Member) *membership.RaftCluster {
	c := membership.NewCluster(zaptest.NewLogger(t))
	for _, m := range membs {
		c.AddMember(m, true)
	}
	return c
}

type nopTransporter struct{}

func newNopTransporter() rafthttp.Transporter {
	return &nopTransporter{}
}

func (s *nopTransporter) Start() error                        { return nil }
func (s *nopTransporter) Handler() http.Handler               { return nil }
func (s *nopTransporter) Send(m []raftpb.Message)             {}
func (s *nopTransporter) SendSnapshot(m snap.Message)         {}
func (s *nopTransporter) AddRemote(id types.ID, us []string)  {}
func (s *nopTransporter) AddPeer(id types.ID, us []string)    {}
func (s *nopTransporter) RemovePeer(id types.ID)              {}
func (s *nopTransporter) RemoveAllPeers()                     {}
func (s *nopTransporter) UpdatePeer(id types.ID, us []string) {}
func (s *nopTransporter) ActiveSince(id types.ID) time.Time   { return time.Time{} }
func (s *nopTransporter) ActivePeers() int                    { return 0 }
func (s *nopTransporter) Stop()                               {}
func (s *nopTransporter) Pause()                              {}
func (s *nopTransporter) Resume()                             {}

type snapTransporter struct {
	nopTransporter
	snapDoneC chan snap.Message
	snapDir   string
	lg        *zap.Logger
}

func newSnapTransporter(lg *zap.Logger, snapDir string) (rafthttp.Transporter, <-chan snap.Message) {
	ch := make(chan snap.Message, 1)
	tr := &snapTransporter{snapDoneC: ch, snapDir: snapDir, lg: lg}
	return tr, ch
}

func (s *snapTransporter) SendSnapshot(m snap.Message) {
	ss := snap.New(s.lg, s.snapDir)
	ss.SaveDBFrom(m.ReadCloser, m.Snapshot.Metadata.Index+1)
	m.CloseWithError(nil)
	s.snapDoneC <- m
}

type sendMsgAppRespTransporter struct {
	nopTransporter
	sendC chan int
}

func newSendMsgAppRespTransporter() (rafthttp.Transporter, <-chan int) {
	ch := make(chan int, 1)
	tr := &sendMsgAppRespTransporter{sendC: ch}
	return tr, ch
}

func (s *sendMsgAppRespTransporter) Send(m []raftpb.Message) {
	var send int
	for _, msg := range m {
		if msg.To != 0 {
			send++
		}
	}
	s.sendC <- send
}

func TestWaitAppliedIndex(t *testing.T) {
	cases := []struct {
		name           string
		appliedIndex   uint64
		committedIndex uint64
		action         func(s *EtcdServer)
		ExpectedError  error
	}{
		{
			name:           "The applied Id is already equal to the commitId",
			appliedIndex:   10,
			committedIndex: 10,
			action: func(s *EtcdServer) {
				s.applyWait.Trigger(10)
			},
			ExpectedError: nil,
		},
		{
			name:           "The etcd server has already stopped",
			appliedIndex:   10,
			committedIndex: 12,
			action: func(s *EtcdServer) {
				s.stopping <- struct{}{}
			},
			ExpectedError: errors.ErrStopped,
		},
		{
			name:           "Timed out waiting for the applied index",
			appliedIndex:   10,
			committedIndex: 12,
			action:         nil,
			ExpectedError:  errors.ErrTimeoutWaitAppliedIndex,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &EtcdServer{
				appliedIndex:   tc.appliedIndex,
				committedIndex: tc.committedIndex,
				stopping:       make(chan struct{}, 1),
				applyWait:      wait.NewTimeList(),
			}

			if tc.action != nil {
				go tc.action(s)
			}

			err := s.waitAppliedIndex()

			if err != tc.ExpectedError {
				t.Errorf("Unexpected error, want (%v), got (%v)", tc.ExpectedError, err)
			}
		})
	}
}
