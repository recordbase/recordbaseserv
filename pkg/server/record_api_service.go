/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package server

import (
	"context"
	"github.com/go-errors/errors"
	"github.com/hashicorp/raft"
	"github.com/recordbase/recordbasepb"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
)

func (t *implAPIServer) GetInfo(ctx context.Context, req *recordbasepb.TenantRequest) (metadata *recordbasepb.Info, err error) {

	metadata = new(recordbasepb.Info)
	err = t.doAuthorized(ctx, "GetInfo", func(ctx context.Context) error {
		metadata, err = t.RecordService.GetInfo(ctx, req.Tenant)
		return err
	})

	return
}

func (t *implAPIServer) Lookup(ctx context.Context, req *recordbasepb.LookupRequest) (entry *recordbasepb.RecordEntry, err error) {

	entry = new(recordbasepb.RecordEntry)
	err = t.doAuthorized(ctx, "Lookup", func(ctx context.Context) error {

		switch req.LookupType {
		case recordbasepb.LookupType_BY_PRIMARY_KEY:
			entry, err = t.RecordService.GetRecord(ctx, req.Tenant, req.Key)
			return err
		case recordbasepb.LookupType_BY_ATTRIBUTE:
			entry, err = t.RecordService.LookupRecord(ctx, req.Tenant, req.Name, req.Key)
			return err
		default:
			return errors.Errorf("unknown lookup type '%s'", req.LookupType.String())
		}

	})

	return
}

func (t *implAPIServer) Search(req *recordbasepb.SearchRequest, stream recordbasepb.RecordService_SearchServer) error {

	return t.doAuthorized(stream.Context(), "Search", func(ctx context.Context) error {

		return t.RecordService.Search(ctx, req.Tenant, req.Name, req.Key, func(entry *recordbasepb.RecordEntry) bool {
			return stream.Send(entry) == nil
		})
		
	})

}

func (t *implAPIServer) Get(ctx context.Context, req *recordbasepb.GetRequest) (entry *recordbasepb.RecordEntry, err error) {

	entry = new(recordbasepb.RecordEntry)
	err = t.doAuthorized(ctx, "Get", func(ctx context.Context) error {
		entry, err = t.RecordService.GetRecord(ctx, req.Tenant, req.PrimaryKey)
		return err
	})

	return
}

func (t *implAPIServer) Create(ctx context.Context,req  *recordbasepb.CreateRequest) (entry *recordbasepb.CreateResponse, err error) {

	entry = new(recordbasepb.CreateResponse)
	err = t.doAuthorized(ctx, "Delete", func(ctx context.Context) error {

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_CREATE_OP,
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

func (t *implAPIServer) Delete(ctx context.Context, req *recordbasepb.DeleteRequest) (*emptypb.Empty, error) {

	return empty, t.doAuthorized(ctx, "Delete", func(ctx context.Context) error {

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_DELETE_OP,
			DeleteReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}

func (t *implAPIServer) Update(ctx context.Context, req *recordbasepb.UpdateRequest) (*emptypb.Empty, error) {

	return empty, t.doAuthorized(ctx, "Update", func(ctx context.Context) error {

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_UPDATE_OP,
			UpdateReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}

func (t *implAPIServer) UploadFile(stream recordbasepb.RecordService_UploadFileServer) error {

	return t.doAuthorized(stream.Context(), "UploadFile", func(ctx context.Context) error {
		return t.doUploadFile(stream)
	})
}

func (t *implAPIServer) doUploadFile(stream recordbasepb.RecordService_UploadFileServer) error {

	var chunk int32
	var last bool
	var firstEntry recordbasepb.UploadFileContent

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

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_UPLOAD_FILE_OP,
			UploadFileReq: &recordbasepb.UploadFileRequest{
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

func (t *implAPIServer) DownloadFile(req *recordbasepb.DownloadFileRequest, stream recordbasepb.RecordService_DownloadFileServer) error {

	return t.doAuthorized(stream.Context(), "DownloadFile", func(ctx context.Context) error {
		var sentErr error
		err := t.RecordService.DownloadFile(ctx, req.Tenant, req.PrimaryKey, req.FileName, func(content *recordbasepb.FileContent) bool {
			sentErr = stream.Send(content)
			return sentErr == nil
		})
		if sentErr != nil {
			err = sentErr
		}
		return err
	})

}

func (t *implAPIServer) DeleteFile(ctx context.Context, req *recordbasepb.DeleteFileRequest) (*emptypb.Empty, error) {

	return empty, t.doAuthorized(ctx, "DeleteFile", func(ctx context.Context) error {

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_DELETE_FILE_OP,
			DeleteFileReq: req,
		}

		return t.updateMethod(ctx, cmd)
	})

}


func (t *implAPIServer) Scan(req *recordbasepb.ScanRequest, stream recordbasepb.RecordService_ScanServer) error {

	return t.doAuthorized(stream.Context(), "Scan", func(ctx context.Context) error {

		return t.RecordService.Scan(ctx, req.Tenant, req.Prefix, int(req.Offset), int(req.Limit), func(entry *recordbasepb.RecordEntry) bool {
			return stream.Send(entry) == nil
		})

	})

}

func (t *implAPIServer) AddKeyRange(ctx context.Context, req *recordbasepb.KeyRange) (*emptypb.Empty, error) {

	return empty, t.doAuthorized(ctx, "AddKeyRange", func(ctx context.Context) error {

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_KEY_RANGE_OP,
			KeyRange: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}

func (t *implAPIServer) GetKeyCapacity(ctx context.Context, req *recordbasepb.TenantRequest) (resp *recordbasepb.KeyCapacity, err error) {

	resp = new(recordbasepb.KeyCapacity)
	err = t.doAuthorized(ctx, "GetKeyCapacity", func(ctx context.Context) error {
		resp, err = t.RecordService.GetKeyCapacity(ctx, req.Tenant)
		return err
	})

	return
}

func (t *implAPIServer) MapGet(ctx context.Context, req *recordbasepb.MapGetRequest) (resp *recordbasepb.MapEntry, err error) {

	resp = new(recordbasepb.MapEntry)
	err = t.doAuthorized(ctx, "MapGet", func(ctx context.Context) error {
		resp, err = t.RecordService.MapGet(ctx, req.Tenant, req.PrimaryKey, req.MapKey)
		return err
	})

	return
}

func (t *implAPIServer) MapPut(ctx context.Context, req *recordbasepb.MapPutRequest) (resp *emptypb.Empty, err error) {

	return empty, t.doAuthorized(ctx, "MapPut", func(ctx context.Context) error {

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_MAP_PUT,
			MapPutReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})
}

func (t *implAPIServer) MapRemove(ctx context.Context, req *recordbasepb.MapRemoveRequest) (resp *emptypb.Empty, err error) {

	return empty, t.doAuthorized(ctx, "MapRemove", func(ctx context.Context) error {

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_MAP_REMOVE,
			MapRemoveReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}

func (t *implAPIServer) MapRange(req *recordbasepb.MapRangeRequest, stream recordbasepb.RecordService_MapRangeServer) error {

	return t.doAuthorized(stream.Context(), "MapRange", func(ctx context.Context) error {

		var sentErr error
		err := t.RecordService.MapRange(ctx, req.Tenant, req.PrimaryKey, func(entry *recordbasepb.MapEntry) bool {
			sentErr = stream.Send(entry)
			return sentErr == nil
		})

		if sentErr != nil {
			err = sentErr
		}

		return err
	})

}

func (t *implAPIServer) BinGet(ctx context.Context, req *recordbasepb.BinGetRequest) (resp *recordbasepb.BinEntry, err error) {

	resp = new(recordbasepb.BinEntry)
	err = t.doAuthorized(ctx, "BinGet", func(ctx context.Context) error {
		resp, err = t.RecordService.BinGet(ctx, req.Tenant, req.PrimaryKey, req.BinName)
		return err
	})

	return
}

func (t *implAPIServer) BinPut(ctx context.Context, req *recordbasepb.BinPutRequest) (resp *emptypb.Empty, err error) {

	return empty, t.doAuthorized(ctx, "BinPut", func(ctx context.Context) error {

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_BIN_PUT,
			BinPutReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})
}

func (t *implAPIServer) BinRemove(ctx context.Context, req *recordbasepb.BinRemoveRequest) (resp *emptypb.Empty, err error) {

	return empty, t.doAuthorized(ctx, "BinRemove", func(ctx context.Context) error {

		cmd := &recordbasepb.Command{
			Operation: recordbasepb.CommandOperation_BIN_REMOVE,
			BinRemoveReq: req,
		}

		return t.updateMethod(ctx, cmd)

	})

}
