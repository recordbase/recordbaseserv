/*
 * Copyright (c) 2022-2023 Zander Schwid & Co. LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package service

import (
	"context"
	"github.com/codeallergy/raftapi"
	"github.com/codeallergy/raftpb"
	"github.com/codeallergy/store"
	"github.com/recordbase/recordpb"
	"github.com/recordbase/recordbaseserv/pkg/api"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"io"
	"time"
)

type implRaftService struct {

	Log             *zap.Logger          `inject`

	RecordStore   store.ManagedDataStore `inject:"bean=record-storage"`
	RecordService api.RecordService      `inject`
}

type RaftUpdateKey struct {
}

func RaftService() raftapi.RaftService {
	return &implRaftService{}
}

func (t *implRaftService) PostConstruct() error {
	return nil
}

// Apply applies a Raft log entry to the key-value store.
func (t *implRaftService) Apply(l *raft.Log) interface{} {

	defer func() {
		if r := recover(); r != nil {
			t.Log.Error("Apply",
				zap.Any("recover", r))
		}
	}()

	cmd := new(recordpb.Command)
	if err := proto.Unmarshal(l.Data, cmd); err != nil {
		t.Log.Error("ApplyUnmarshal", zap.Error(err))
		return err
	}
	return t.applyCommand(cmd)
}

func (t *implRaftService) applyCommand(cmd *recordpb.Command) interface{} {

	ctx := context.WithValue(context.Background(), RaftUpdateKey{}, true)

	status, err := t.doApplyCommand(ctx, cmd)
	return raftapi.FSMResponse{Status: status, Err: err}
}

func (t *implRaftService) doApplyCommand(ctx context.Context, cmd *recordpb.Command) (*raftpb.Status, error) {
	switch cmd.Operation {
	case recordpb.CommandOperation_CREATE_OP:
		return t.RecordService.CreateRecord(ctx, cmd.CreateReq)
	case recordpb.CommandOperation_UPDATE_OP:
		return t.RecordService.UpdateRecord(ctx, cmd.UpdateReq)
	case recordpb.CommandOperation_DELETE_OP:
		return t.RecordService.DeleteRecord(ctx, cmd.DeleteReq)
	case recordpb.CommandOperation_ADD_RANGE_OP:
		return t.RecordService.AddKeyRange(ctx, cmd.KeyRange)
	default:
		return &raftpb.Status{
			Updated: false,
		}, errors.Errorf("unknown command %s", cmd.Operation.String())
	}
}

func (t *implRaftService) Snapshot() (raft.FSMSnapshot, error) {
	return fsmSnapshot{store: t.RecordStore, log: t.Log }, nil
}

type fsmSnapshot struct {
	store    store.ManagedDataStore
	log      *zap.Logger
}

func (t fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	start := time.Now()
	lastTx, err := t.store.Backup(sink, 0)
	elapsed := time.Since(start).Seconds()
	if err != nil {
		t.log.Error("SnapshotPersist", zap.Float64("elapsed", elapsed), zap.Error(err))
	} else {
		t.log.Info("SnapshotPersisted", zap.Float64("elapsed", elapsed), zap.Uint64("lastTx", lastTx))
	}
	return err
}

func (t fsmSnapshot) Release() {
	// do nothing
}

func (t *implRaftService) Restore(snapshot io.ReadCloser) (err error) {
	start := time.Now()
	defer snapshot.Close()
	err = t.RecordStore.DropAll()
	if err == nil {
		err = t.RecordStore.Restore(snapshot)
	}
	elapsed := time.Since(start).Seconds()
	if err != nil {
		t.Log.Error("SnapshotRestore", zap.Float64("elapsed", elapsed), zap.Error(err))
	} else {
		t.Log.Info("SnapshotRestored", zap.Float64("elapsed", elapsed))
	}
	return err
}
