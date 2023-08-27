/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package service

import (
	"context"
	"github.com/openraft/raftapi"
	"github.com/openraft/raftpb"
	"github.com/keyvalstore/store"
	"github.com/recordbase/recordbasepb"
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

	cmd := new(recordbasepb.Command)
	if err := proto.Unmarshal(l.Data, cmd); err != nil {
		t.Log.Error("ApplyUnmarshal", zap.Error(err))
		return err
	}
	return t.applyCommand(cmd)
}

func (t *implRaftService) applyCommand(cmd *recordbasepb.Command) interface{} {

	ctx := context.WithValue(context.Background(), RaftUpdateKey{}, true)

	status, err := t.doApplyCommand(ctx, cmd)
	return raftapi.FSMResponse{Status: status, Err: err}
}

func (t *implRaftService) doApplyCommand(ctx context.Context, cmd *recordbasepb.Command) (*raftpb.Status, error) {
	switch cmd.Operation {
	case recordbasepb.CommandOperation_CREATE_OP:
		return t.RecordService.CreateRecord(ctx, cmd.CreateReq)
	case recordbasepb.CommandOperation_UPDATE_OP:
		return t.RecordService.UpdateRecord(ctx, cmd.UpdateReq)
	case recordbasepb.CommandOperation_DELETE_OP:
		return t.RecordService.DeleteRecord(ctx, cmd.DeleteReq)
	case recordbasepb.CommandOperation_UPLOAD_FILE_OP:
		return t.RecordService.UploadFile(ctx, cmd.UploadFileReq)
	case recordbasepb.CommandOperation_DELETE_FILE_OP:
		return t.RecordService.DeleteFile(ctx, cmd.DeleteFileReq)
	case recordbasepb.CommandOperation_KEY_RANGE_OP:
		return t.RecordService.AddKeyRange(ctx, cmd.KeyRange)
	case recordbasepb.CommandOperation_MAP_PUT:
		return t.RecordService.MapPut(ctx, cmd.MapPutReq)
	case recordbasepb.CommandOperation_MAP_REMOVE:
		return t.RecordService.MapRemove(ctx, cmd.MapRemoveReq)
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
