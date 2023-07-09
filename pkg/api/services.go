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

package api

import (
	"context"
	"github.com/openraft/raftpb"
	"github.com/recordbase/recordpb"
	"reflect"
)

var RecordServiceClass = reflect.TypeOf((*RecordService)(nil)).Elem()

type RecordService interface {

	GetInfo(ctx context.Context, tenant string) (*recordpb.Info, error)

	GetRecord(ctx context.Context, tenant, primaryKey string) (*recordpb.RecordEntry, error)

	LookupRecord(ctx context.Context, tenant, attribute, primaryKey string) (*recordpb.RecordEntry, error)

	Search(ctx context.Context, tenant, attribute, primaryKey string, cb func(*recordpb.RecordEntry) bool) error

	Scan(ctx context.Context, tenant, prefix string, offset, limit int, cb func(*recordpb.RecordEntry) bool) error

	DownloadFile(ctx context.Context, tenant, primaryKey, fileName string, cb func(*recordpb.FileContent) bool) error

	MapGet(ctx context.Context, tenant, primaryKey, mapKey string) (*recordpb.MapEntry, error)

	MapRange(ctx context.Context, tenant, primaryKey string, cb func(entry *recordpb.MapEntry) bool) error

	BinGet(ctx context.Context, tenant, primaryKey, binName string) (*recordpb.BinEntry, error)

	/**
	Raft update methods
	 */

	CreateRecord(ctx context.Context, request *recordpb.CreateRequest) (*raftpb.Status, error)

	UpdateRecord(ctx context.Context, request *recordpb.UpdateRequest) (*raftpb.Status, error)

	DeleteRecord(ctx context.Context, request *recordpb.DeleteRequest) (*raftpb.Status, error)

	UploadFile(ctx context.Context, request *recordpb.UploadFileRequest) (*raftpb.Status, error)

	DeleteFile(ctx context.Context, request *recordpb.DeleteFileRequest) (*raftpb.Status, error)

	AddKeyRange(ctx context.Context, in *recordpb.KeyRange) (*raftpb.Status, error)

	MapPut(ctx context.Context, request *recordpb.MapPutRequest) (*raftpb.Status, error)

	MapRemove(ctx context.Context, request *recordpb.MapRemoveRequest) (*raftpb.Status, error)

	BinPut(ctx context.Context, request *recordpb.BinPutRequest) (*raftpb.Status, error)

	BinRemove(ctx context.Context, request *recordpb.BinRemoveRequest) (*raftpb.Status, error)

	/**
	ID allocation
	 */

	GetKeyCapacity(ctx context.Context, tenant string) (*recordpb.KeyCapacity, error)

}


