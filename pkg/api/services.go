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
	"github.com/codeallergy/raftpb"
	"github.com/recordbase/recordpb"
	"reflect"
)

var RecordServiceClass = reflect.TypeOf((*RecordService)(nil)).Elem()

type RecordService interface {

	GetCounts(ctx context.Context, tenant string) (*recordpb.Counts, error)

	GetRecord(ctx context.Context, tenant, userId string, withFileContents bool) (*recordpb.RecordEntry, error)

	LookupRecord(ctx context.Context, tenant, attribute, key string, withFileContents bool) (*recordpb.RecordEntry, error)

	Search(ctx context.Context, tenant, attribute, key string, cb func(*recordpb.RecordEntry) bool) error

	Scan(ctx context.Context, tenant, prefix string, offset, limit int, cb func(*recordpb.RecordEntry) bool) error

	/**
	Raft update methods
	 */

	CreateRecord(ctx context.Context, request *recordpb.CreateRequest) (*raftpb.Status, error)

	UpdateRecord(ctx context.Context, request *recordpb.UpdateRequest) (*raftpb.Status, error)

	DeleteRecord(ctx context.Context, request *recordpb.DeleteRequest) (*raftpb.Status, error)

	AddKeyRange(ctx context.Context, in *recordpb.KeyRange) (*raftpb.Status, error)

	/**
	ID allocation
	 */

	GetKeyCapacity(ctx context.Context, tenant string) (*recordpb.KeyCapacity, error)

}


