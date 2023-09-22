/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package api

import (
	"context"
	"github.com/sprintframework/raftpb"
	"github.com/recordbase/recordbasepb"
	"reflect"
)

var RecordServiceClass = reflect.TypeOf((*RecordService)(nil)).Elem()

type RecordService interface {

	GetInfo(ctx context.Context, tenant string) (*recordbasepb.Info, error)

	GetRecord(ctx context.Context, tenant, primaryKey string) (*recordbasepb.RecordEntry, error)

	LookupRecord(ctx context.Context, tenant, attribute, primaryKey string) (*recordbasepb.RecordEntry, error)

	Search(ctx context.Context, tenant, attribute, primaryKey string, cb func(*recordbasepb.RecordEntry) bool) error

	Scan(ctx context.Context, tenant, prefix string, offset, limit int, cb func(*recordbasepb.RecordEntry) bool) error

	DownloadFile(ctx context.Context, tenant, primaryKey, fileName string, cb func(*recordbasepb.FileContent) bool) error

	MapGet(ctx context.Context, tenant, primaryKey, mapKey string) (*recordbasepb.MapEntry, error)

	MapRange(ctx context.Context, tenant, primaryKey string, cb func(entry *recordbasepb.MapEntry) bool) error

	BinGet(ctx context.Context, tenant, primaryKey, binName string) (*recordbasepb.BinEntry, error)

	/**
	Raft update methods
	 */

	CreateRecord(ctx context.Context, request *recordbasepb.CreateRequest) (*raftpb.Status, error)

	UpdateRecord(ctx context.Context, request *recordbasepb.UpdateRequest) (*raftpb.Status, error)

	DeleteRecord(ctx context.Context, request *recordbasepb.DeleteRequest) (*raftpb.Status, error)

	UploadFile(ctx context.Context, request *recordbasepb.UploadFileRequest) (*raftpb.Status, error)

	DeleteFile(ctx context.Context, request *recordbasepb.DeleteFileRequest) (*raftpb.Status, error)

	AddKeyRange(ctx context.Context, in *recordbasepb.KeyRange) (*raftpb.Status, error)

	MapPut(ctx context.Context, request *recordbasepb.MapPutRequest) (*raftpb.Status, error)

	MapRemove(ctx context.Context, request *recordbasepb.MapRemoveRequest) (*raftpb.Status, error)

	BinPut(ctx context.Context, request *recordbasepb.BinPutRequest) (*raftpb.Status, error)

	BinRemove(ctx context.Context, request *recordbasepb.BinRemoveRequest) (*raftpb.Status, error)

	/**
	ID allocation
	 */

	GetKeyCapacity(ctx context.Context, tenant string) (*recordbasepb.KeyCapacity, error)

}


