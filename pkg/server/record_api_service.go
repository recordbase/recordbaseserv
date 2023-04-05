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

package server

import (
	"context"
	"github.com/recordbase/recordpb"
	"github.com/go-errors/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (t *implAPIServer) GetCounts(ctx context.Context, req *recordpb.TenantRequest) (metadata *recordpb.Counts, err error) {

	metadata = new(recordpb.Counts)
	err = t.doAuthorized(ctx, "GetCounts", func(ctx context.Context) error {
		metadata, err = t.UserService.GetCounts(ctx, req.Tenant)
		return err
	})

	return
}

func (t *implAPIServer) Lookup(ctx context.Context, req *recordpb.LookupRequest) (entry *recordpb.RecordEntry, err error) {

	entry = new(recordpb.RecordEntry)
	err = t.doAuthorized(ctx, "Lookup", func(ctx context.Context) error {

		switch req.LookupType {
		case recordpb.LookupType_BY_PRIMARY_KEY:
			entry, err = t.UserService.GetRecord(ctx, req.Tenant, req.Key, req.FileContents)
			return err
		case recordpb.LookupType_BY_ATTRIBUTE:
			entry, err = t.UserService.LookupRecord(ctx, req.Tenant, req.Name, req.Key, req.FileContents)
			return err
		default:
			return errors.Errorf("unknown lookup type '%s'", req.LookupType.String())
		}

	})

	return
}

func (t *implAPIServer) Search(req *recordpb.SearchRequest, stream recordpb.RecordService_SearchServer) error {

	return t.doAuthorized(stream.Context(), "Search", func(ctx context.Context) error {

		return t.UserService.Search(ctx, req.Tenant, req.Name, req.Key, func(entry *recordpb.RecordEntry) bool {
			return stream.Send(entry) == nil
		})
		
	})

}

func (t *implAPIServer) Get(ctx context.Context, req *recordpb.GetRequest) (entry *recordpb.RecordEntry, err error) {

	entry = new(recordpb.RecordEntry)
	err = t.doAuthorized(ctx, "Get", func(ctx context.Context) error {
		entry, err = t.UserService.GetRecord(ctx, req.Tenant, req.PrimaryKey, req.FileContents)
		return err
	})

	return
}

func (t *implAPIServer) Create(ctx context.Context,req  *recordpb.CreateRequest) (entry *recordpb.CreateResponse, err error) {

	cmd := &recordpb.Command{
		Operation: recordpb.CommandOperation_CREATE_OP,
		CreateReq: req,
	}

	entry = new(recordpb.CreateResponse)
	err = t.doWithRaft(ctx, "Create", func(ctx context.Context, r *raft.Raft) error {
		status, err := t.applyCommand(ctx, r, cmd)
		entry.PrimaryKey = status.Id
		return err
	})

	return
}

func (t *implAPIServer) Delete(ctx context.Context, req *recordpb.DeleteRequest) (*emptypb.Empty, error) {

	cmd := &recordpb.Command{
		Operation: recordpb.CommandOperation_DELETE_OP,
		DeleteReq: req,
	}

	return empty, t.updateMethod(ctx, "Delete", cmd)

}

func (t *implAPIServer) Update(ctx context.Context, req *recordpb.UpdateRequest) (*emptypb.Empty, error) {

	cmd := &recordpb.Command{
		Operation: recordpb.CommandOperation_UPDATE_OP,
		UpdateReq: req,
	}

	return empty, t.updateMethod(ctx, "Update", cmd)

}

func (t *implAPIServer) Scan(req *recordpb.ScanRequest, stream recordpb.RecordService_ScanServer) error {

	return t.doAuthorized(stream.Context(), "Scan", func(ctx context.Context) error {

		return t.UserService.Scan(ctx, req.Tenant, req.Prefix, int(req.Offset), int(req.Limit), func(entry *recordpb.RecordEntry) bool {
			return stream.Send(entry) == nil
		})

	})

}

func (t *implAPIServer) AddKeyRange(ctx context.Context, req *recordpb.KeyRange) (*emptypb.Empty, error) {

	cmd := &recordpb.Command{
		Operation: recordpb.CommandOperation_ADD_RANGE_OP,
		KeyRange: req,
	}

	return empty, t.updateMethod(ctx, "AddKeyRange", cmd)

}

func (t *implAPIServer) GetKeyCapacity(ctx context.Context, req *recordpb.TenantRequest) (resp *recordpb.KeyCapacity, err error) {

	resp = new(recordpb.KeyCapacity)
	err = t.doAuthorized(ctx, "GetKeyCapacity", func(ctx context.Context) error {
		resp, err = t.UserService.GetKeyCapacity(ctx, req.Tenant)
		return err
	})

	return
}



