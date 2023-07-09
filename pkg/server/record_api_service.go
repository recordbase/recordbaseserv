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
	"github.com/go-errors/errors"
	"github.com/hashicorp/raft"
	"github.com/recordbase/recordpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
)

func (t *implAPIServer) GetInfo(ctx context.Context, req *recordpb.TenantRequest) (metadata *recordpb.Info, err error) {

	metadata = new(recordpb.Info)
	err = t.doAuthorized(ctx, "GetInfo", func(ctx context.Context) error {
		metadata, err = t.RecordService.GetInfo(ctx, req.Tenant)
		return err
	})

	return
}

func (t *implAPIServer) Lookup(ctx context.Context, req *recordpb.LookupRequest) (entry *recordpb.RecordEntry, err error) {

	entry = new(recordpb.RecordEntry)
	err = t.doAuthorized(ctx, "Lookup", func(ctx context.Context) error {

		switch req.LookupType {
		case recordpb.LookupType_BY_PRIMARY_KEY:
			entry, err = t.RecordService.GetRecord(ctx, req.Tenant, req.Key)
			return err
		case recordpb.LookupType_BY_ATTRIBUTE:
			entry, err = t.RecordService.LookupRecord(ctx, req.Tenant, req.Name, req.Key)
			return err
		default:
			return errors.Errorf("unknown lookup type '%s'", req.LookupType.String())
		}

	})

	return
}

func (t *implAPIServer) Search(req *recordpb.SearchRequest, stream recordpb.RecordService_SearchServer) error {

	return t.doAuthorized(stream.Context(), "Search", func(ctx context.Context) error {

		return t.RecordService.Search(ctx, req.Tenant, req.Name, req.Key, func(entry *recordpb.RecordEntry) bool {
			return stream.Send(entry) == nil
		})
		
	})

}

func (t *implAPIServer) Get(ctx context.Context, req *recordpb.GetRequest) (entry *recordpb.RecordEntry, err error) {

	entry = new(recordpb.RecordEntry)
	err = t.doAuthorized(ctx, "Get", func(ctx context.Context) error {
		entry, err = t.RecordService.GetRecord(ctx, req.Tenant, req.PrimaryKey)
		return err
	})

	return
}

func (t *implAPIServer) Create(ctx context.Context,req  *recordpb.CreateRequest) (entry *recordpb.CreateResponse, err error) {

	entry = new(recordpb.CreateResponse)
	err = t.doAuthorized(ctx, "Delete", func(ctx context.Context) error {

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_CREATE_OP,
			CreateReq: req,
		}

		return t.doWithRaft(ctx, func(ctx context.Context, r *raft.Raft) error {
			status, err := t.applyCommand(ctx, r, cmd)
			entry.PrimaryKey = status.Id
			return err
		})

	})

	return
}

func (t *implAPIServer) Delete(ctx context.Context, req *recordpb.DeleteRequest) (*emptypb.Empty, error) {

	return empty, t.doAuthorized(ctx, "Delete", func(ctx context.Context) error {

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_DELETE_OP,
			DeleteReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}

func (t *implAPIServer) Update(ctx context.Context, req *recordpb.UpdateRequest) (*emptypb.Empty, error) {

	return empty, t.doAuthorized(ctx, "Update", func(ctx context.Context) error {

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_UPDATE_OP,
			UpdateReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}

func (t *implAPIServer) UploadFile(stream recordpb.RecordService_UploadFileServer) error {

	return t.doAuthorized(stream.Context(), "UploadFile", func(ctx context.Context) error {
		return t.doUploadFile(stream)
	})
}

func (t *implAPIServer) doUploadFile(stream recordpb.RecordService_UploadFileServer) error {

	var chunk int32
	var last bool
	var firstEntry recordpb.UploadFileContent

	for stream.Context().Err() == nil {
		entry, err := stream.Recv()
		if err == io.EOF {
			last = true
			entry = &firstEntry
			if entry.PrimaryKey == "" {
				return err
			}
		} else if err != nil {
			return err
		}

		if chunk == 0 {
			firstEntry = *entry
		}

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_UPLOAD_FILE_OP,
			UploadFileReq: &recordpb.UploadFileRequest{
				Tenant:     entry.Tenant,
				PrimaryKey: entry.PrimaryKey,
				FileName:   entry.FileName,
				Data:       entry.Data,
				Chunk:      chunk,
				Last:       last,
			},
		}

		chunk++

		err = t.updateMethod(stream.Context(), cmd)
		if err != nil {
			return err
		}

		if last {
			break
		}
	}

	return nil

}

func (t *implAPIServer) DownloadFile(req *recordpb.DownloadFileRequest, stream recordpb.RecordService_DownloadFileServer) error {

	return t.doAuthorized(stream.Context(), "DownloadFile", func(ctx context.Context) error {
		var sentErr error
		err := t.RecordService.DownloadFile(ctx, req.Tenant, req.PrimaryKey, req.FileName, func(content *recordpb.FileContent) bool {
			sentErr = stream.Send(content)
			return sentErr == nil
		})
		if sentErr != nil {
			err = sentErr
		}
		return err
	})

}

func (t *implAPIServer) DeleteFile(ctx context.Context, req *recordpb.DeleteFileRequest) (*emptypb.Empty, error) {

	return empty, t.doAuthorized(ctx, "DeleteFile", func(ctx context.Context) error {

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_DELETE_FILE_OP,
			DeleteFileReq: req,
		}

		return t.updateMethod(ctx, cmd)
	})

}


func (t *implAPIServer) Scan(req *recordpb.ScanRequest, stream recordpb.RecordService_ScanServer) error {

	return t.doAuthorized(stream.Context(), "Scan", func(ctx context.Context) error {

		return t.RecordService.Scan(ctx, req.Tenant, req.Prefix, int(req.Offset), int(req.Limit), func(entry *recordpb.RecordEntry) bool {
			return stream.Send(entry) == nil
		})

	})

}

func (t *implAPIServer) AddKeyRange(ctx context.Context, req *recordpb.KeyRange) (*emptypb.Empty, error) {

	return empty, t.doAuthorized(ctx, "AddKeyRange", func(ctx context.Context) error {

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_KEY_RANGE_OP,
			KeyRange: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}

func (t *implAPIServer) GetKeyCapacity(ctx context.Context, req *recordpb.TenantRequest) (resp *recordpb.KeyCapacity, err error) {

	resp = new(recordpb.KeyCapacity)
	err = t.doAuthorized(ctx, "GetKeyCapacity", func(ctx context.Context) error {
		resp, err = t.RecordService.GetKeyCapacity(ctx, req.Tenant)
		return err
	})

	return
}

func (t *implAPIServer) MapGet(ctx context.Context, req *recordpb.MapGetRequest) (resp *recordpb.MapEntry, err error) {

	resp = new(recordpb.MapEntry)
	err = t.doAuthorized(ctx, "MapGet", func(ctx context.Context) error {
		resp, err = t.RecordService.MapGet(ctx, req.Tenant, req.PrimaryKey, req.MapKey)
		return err
	})

	return
}

func (t *implAPIServer) MapPut(ctx context.Context, req *recordpb.MapPutRequest) (resp *emptypb.Empty, err error) {

	return empty, t.doAuthorized(ctx, "MapPut", func(ctx context.Context) error {

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_MAP_PUT,
			MapPutReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})
}

func (t *implAPIServer) MapRemove(ctx context.Context, req *recordpb.MapRemoveRequest) (resp *emptypb.Empty, err error) {

	return empty, t.doAuthorized(ctx, "MapRemove", func(ctx context.Context) error {

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_MAP_REMOVE,
			MapRemoveReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}

func (t *implAPIServer) MapRange(req *recordpb.MapRangeRequest, stream recordpb.RecordService_MapRangeServer) error {

	return t.doAuthorized(stream.Context(), "MapRange", func(ctx context.Context) error {

		var sentErr error
		err := t.RecordService.MapRange(ctx, req.Tenant, req.PrimaryKey, func(entry *recordpb.MapEntry) bool {
			sentErr = stream.Send(entry)
			return sentErr == nil
		})

		if sentErr != nil {
			err = sentErr
		}

		return err
	})

}

func (t *implAPIServer) BinGet(ctx context.Context, req *recordpb.BinGetRequest) (resp *recordpb.BinEntry, err error) {

	resp = new(recordpb.BinEntry)
	err = t.doAuthorized(ctx, "BinGet", func(ctx context.Context) error {
		resp, err = t.RecordService.BinGet(ctx, req.Tenant, req.PrimaryKey, req.BinName)
		return err
	})

	return
}

func (t *implAPIServer) BinPut(ctx context.Context, req *recordpb.BinPutRequest) (resp *emptypb.Empty, err error) {

	return empty, t.doAuthorized(ctx, "BinPut", func(ctx context.Context) error {

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_BIN_PUT,
			BinPutReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})
}

func (t *implAPIServer) BinRemove(ctx context.Context, req *recordpb.BinRemoveRequest) (resp *emptypb.Empty, err error) {

	return empty, t.doAuthorized(ctx, "BinRemove", func(ctx context.Context) error {

		cmd := &recordpb.Command{
			Operation: recordpb.CommandOperation_BIN_REMOVE,
			BinRemoveReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}
