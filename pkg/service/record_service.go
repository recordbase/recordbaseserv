/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package service

import (
	"context"
	"fmt"
	"github.com/sprintframework/raftpb"
	"github.com/keyvalstore/store"
	"github.com/recordbase/recordbasepb"
	"github.com/recordbase/recordbaseserv/pkg/api"
	"github.com/go-errors/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"sort"
	"strings"
	"time"
)

var (
	ErrNotFound = status.Error(codes.NotFound, "entity not found")
	ErrOutOfRange = status.Error(codes.OutOfRange, "id out of range")
	ErrCollision = status.Error(codes.AlreadyExists, "collision with digest")
	ErrConcurrentUpdate = status.Error(codes.ResourceExhausted, "concurrent update")
)

type implRecordService struct {

	Log             *zap.Logger          `inject`

	RecordStore          store.ManagedDataStore     `inject:"bean=record-store"`
	TransactionalManager store.TransactionalManager `inject:"bean=record-store"`

	DeleteGracePeriod        int  `value:"record.delete-grace-period,default=45"`
	StripUnknownChars        bool `value:"record.strip-unknown-chars,default=false"`
	IgnoreDuplicateEntries   bool `value:"record.ignore-duplicate-entries,default=false"`
}

func RecordService() api.RecordService {
	return &implRecordService{}
}

func (t *implRecordService) GetRecord(ctx context.Context, tenant, primaryKey string) (entity *recordbasepb.RecordEntry, err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, true)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	entity = new(recordbasepb.RecordEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", tenant, primaryKey).ToProto(entity)

	if entity.PrimaryKey != primaryKey {
		return nil, ErrNotFound
	}

	return
}

func (t *implRecordService) BinGet(ctx context.Context, tenant, primaryKey, binName string) (*recordbasepb.BinEntry, error) {

	record := new(recordbasepb.RecordEntry)
	err := t.RecordStore.Get(ctx).ByKey("%s:rec:%s", tenant, primaryKey).ToProto(record)
	if err != nil {
		return nil, err
	}

	for _, bin := range record.Bins {
		if bin.Name == binName {
			return bin, nil
		}
	}

	return nil, ErrNotFound
}

func (t *implRecordService) LookupRecord(ctx context.Context, tenant, attribute, key string) (entity *recordbasepb.RecordEntry, err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, true)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	digest := digestAttribute(attribute, key)

	var primaryKey string
	primaryKey, err = t.RecordStore.Get(ctx).ByKey("%s:attr:%s", tenant, digest).ToString()
	if err != nil {
		return
	}

	if primaryKey == "" {
		return nil, ErrNotFound
	}

	return t.GetRecord(ctx, tenant, primaryKey)
}

func (t *implRecordService) Search(ctx context.Context, tenant, attribute, key string, cb func(*recordbasepb.RecordEntry) bool) (err error) {

	digest := digestAttribute(attribute, key)

	prefix := fmt.Sprintf("%s:attr:%s:", tenant, digest)
	var processingErr error
	err = t.RecordStore.Enumerate(ctx).
		ByPrefix(prefix).
		Do(func(entry *store.RawEntry) bool {
			key := string(entry.Key)
			primaryKey := strings.TrimPrefix(key, prefix)
			entity := new(recordbasepb.RecordEntry)
			processingErr = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", tenant, primaryKey).ToProto(entity)
			if processingErr != nil {
				return false
			}
			if entity.PrimaryKey == "" {
				// user not found, skip
				return true
			}
			if entity.DeletedAt > 0 {
				// user deleted, skip
				return true
			}
			return cb(entity)
	})

	if processingErr != nil {
		err = processingErr
	}

	return
}

func (t *implRecordService) Scan(ctx context.Context, tenant, prefix string, offset, limit int, cb func(*recordbasepb.RecordEntry) bool) error {

	if offset < 0 || limit <= 0 {
		return nil
	}

	return t.RecordStore.Enumerate(ctx).ByPrefix("%s:rec:%s", tenant, prefix).DoProto(func() proto.Message {
		return new(recordbasepb.RecordEntry)
	}, func(entry *store.ProtoEntry) bool {
		if offset > 0 {
			offset--
			return true
		}
		if limit > 0 {
			if value, ok := entry.Value.(*recordbasepb.RecordEntry); ok {
				if value.DeletedAt == 0 {
					limit--
					return cb(value)
				}
			}
			return true
		}
		return false
	})

}


func (t *implRecordService) CreateRecord(ctx context.Context, request *recordbasepb.CreateRequest) (status *raftpb.Status, err error) {

	status = new(raftpb.Status)

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	status.Id, err = t.allocateId(ctx, request.Tenant)
	if err != nil {
		return
	}

	record, err := t.newRecord(request.Tenant, status.Id,
		request.Attributes, request.Tags, request.Bins)
	if err != nil {
		return
	}

	status.Updated = true
	return status, t.doCreateRecord(ctx, record)
}

func (t *implRecordService) newRecord(tenant, primaryKey string,
	attributes []*recordbasepb.AttributeEntry,
	tags []string,
	bins []*recordbasepb.BinEntry) (record *recordbasepb.RecordEntry, err error) {

	current := time.Now().Unix()

	attributes, err = NormalizeAttributes(attributes)
	if err != nil {
		return
	}

	tags, err = NormalizeTags(tags)
	if err != nil {
		return
	}

	bins, err = NormalizeBins(bins)
	if err != nil {
		return
	}

	record = &recordbasepb.RecordEntry{
		Tenant:     tenant,
		PrimaryKey: primaryKey,
		Attributes: attributes,
		Tags:       tags,
		Bins:       bins,
		CreatedAt:  current,
		UpdatedAt:  current,
	}

	return
}

func (t *implRecordService) doCreateRecord(ctx context.Context, entity *recordbasepb.RecordEntry) (err error) {

	visited := make(map[string]bool)
	for _, fileInfo := range entity.Files {
		if visited[fileInfo.Name] {
			if t.IgnoreDuplicateEntries {
				continue
			} else {
				return errors.Errorf("duplicate file '%s' in the record", fileInfo.Name)
			}
		}
		visited[fileInfo.Name] = true
		err = t.RecordStore.Set(ctx).ByKey("%s:file:%s:%s", entity.Tenant, entity.PrimaryKey, digest256(fileInfo.Name)).Binary(fileInfo.Data)
		if err != nil {
			return
		}
		fileInfo.CreatedAt = entity.CreatedAt
		fileInfo.Data = nil
	}

	err = t.RecordStore.Set(ctx).ByKey("%s:rec:%s", entity.Tenant, entity.PrimaryKey).Proto(entity)
	if err != nil {
		return
	}

	visited = make(map[string]bool)
	for _, item := range entity.Attributes {
		if visited[item.Name] {
			if t.IgnoreDuplicateEntries {
				continue
			} else {
				return errors.Errorf("duplicate attribute '%s' in the record", item.Name)
			}
		}
		visited[item.Name] = true
		err = t.indexAttribute(ctx, entity.Tenant, entity.PrimaryKey, item)
		if err != nil {
			return
		}
	}

	visited = make(map[string]bool)
	for _, tag := range entity.Tags {
		if visited[tag] {
			if t.IgnoreDuplicateEntries {
				continue
			} else {
				return errors.Errorf("duplicate tag '%s' in the record", tag)
			}
		}
		visited[tag] = true
		err = t.indexTag(ctx, entity.Tenant, entity.PrimaryKey, tag)
		if err != nil {
			return
		}
	}

	visited = make(map[string]bool)
	for _, bin := range entity.Bins {
		if visited[bin.Name] {
			if t.IgnoreDuplicateEntries {
				continue
			} else {
				return errors.Errorf("duplicate bin '%s' in the record", bin.Name)
			}
		}
		visited[bin.Name] = true

		err = t.incrementCounter(ctx, entity.Tenant, "bin", bin.Name, 1)
		if err != nil {
			return err
		}
	}

	return
}

func (t *implRecordService) UpdateRecord(ctx context.Context, request *recordbasepb.UpdateRequest) (status *raftpb.Status, err error) {

	status = new(raftpb.Status)

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	record := new(recordbasepb.RecordEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", request.Tenant, request.PrimaryKey).ToProto(record)
	if err != nil {
		return
	}

	if record.PrimaryKey == "" || record.DeletedAt > 0 {
		return status, ErrNotFound
	}

	/*
	if record.DeletedAt > 0 {
		status.Id = request.PrimaryKey
		status.Updated = false
		record = new(recordbasepb.RecordEntry)
		err = t.RecordStore.Get(ctx).ByKey("%s:del:%s", request.Tenant, request.PrimaryKey).ToProto(record)
		if err != nil {
			return
		}
		if record.PrimaryKey == "" {
			return status, ErrNotFound
		}
		// record restored
		record.DeletedAt = 0
	}
	*/

	/**
	Update Attributes
	 */

	var preparingAttributes []*recordbasepb.AttributeEntry
	existingAttributes := mapAttributes(record.Attributes)
	requestAttributes := make(map[string]bool)

	for _, item := range request.Attributes {
		if requestAttributes[item.Name] {
			if t.IgnoreDuplicateEntries {
				continue
			} else {
				return nil, errors.Errorf("duplicate attribute '%s' in the record", item.Name)
			}
		}
		requestAttributes[item.Name] = true

		if existingAttr, ok := existingAttributes[item.Name]; ok {
			// replace attribute if value is different
			if item.Value != existingAttr.Value {
				err = t.unindexAttribute(ctx, record.Tenant, record.PrimaryKey, existingAttr)
				if err != nil {
					return status, err
				}
				err = t.indexAttribute(ctx, record.Tenant, record.PrimaryKey, item)
				if err != nil {
					return status, err
				}
			}
		} else {
			// add attribute
			err = t.indexAttribute(ctx, record.Tenant, record.PrimaryKey, item)
			if err != nil {
				return status, err
			}
		}
		preparingAttributes = append(preparingAttributes, item)
	}

	for _, existingAttribute := range record.Attributes {
		if !requestAttributes[existingAttribute.Name] {

			switch request.UpdateType {
			case recordbasepb.UpdateType_MERGE:
				// keep it
				preparingAttributes = append(preparingAttributes, existingAttribute)
			case recordbasepb.UpdateType_REPLACE:
				// remove it
				err = t.unindexAttribute(ctx, record.Tenant, record.PrimaryKey, existingAttribute)
				if err != nil {
					return status, err
				}
			default:
				return status, errors.Errorf("unknown update type '%s'", request.UpdateType.String())

			}

		}
	}

	/**
	Update Tags
	*/

	var preparingTags []string
	existingTags := mapTags(record.Tags)
	requestTags := make(map[string]bool)

	for _, tag := range request.Tags {
		if requestTags[tag] {
			if t.IgnoreDuplicateEntries {
				continue
			} else {
				return nil, errors.Errorf("duplicate tag '%s' in the record", tag)
			}
		}
		requestTags[tag] = true

		if !existingTags[tag] {
			// add tag
			err = t.indexTag(ctx, record.Tenant, record.PrimaryKey, tag)
			if err != nil {
				return status, err
			}
		}
		preparingTags = append(preparingTags, tag)
	}

	for _, existingTag := range record.Tags {
		if !requestTags[existingTag] {

			switch request.UpdateType {
			case recordbasepb.UpdateType_MERGE:
				// keep it
				preparingTags = append(preparingTags, existingTag)
			case recordbasepb.UpdateType_REPLACE:
				// remove it
				err = t.unindexTag(ctx, record.Tenant, record.PrimaryKey, existingTag)
				if err != nil {
					return status, err
				}
			default:
				return status, errors.Errorf("unknown update type '%s'", request.UpdateType.String())
			}

		}
	}

	/**
	Update Bins
	*/

	var preparingBins []*recordbasepb.BinEntry
	existingBins := mapBins(record.Bins)
	requestBins := make(map[string]bool)

	for _, item := range request.Bins {
		if requestBins[item.Name] {
			if t.IgnoreDuplicateEntries {
				continue
			} else {
				return status, errors.Errorf("duplicate bin '%s' in the record", item.Name)
			}
		}
		requestBins[item.Name] = true

		if !existingBins[item.Name] {
			// increment counter
			err = t.incrementCounter(ctx, record.Tenant, "bin", item.Name, 1)
			if err != nil {
				return status, errors.Errorf("increment index on bin '%s' in the record, %v", item.Name, err)
			}
		}

		preparingBins = append(preparingBins, item)
	}

	for _, existingBin := range record.Bins {
		if !requestBins[existingBin.Name] {

			switch request.UpdateType {
			case recordbasepb.UpdateType_MERGE:
				// keep it
				preparingBins = append(preparingBins, existingBin)
			case recordbasepb.UpdateType_REPLACE:
				// decrement counter
				err = t.incrementCounter(ctx, record.Tenant, "bin", existingBin.Name, -1)
				if err != nil {
					return status, errors.Errorf("decrement index on bin '%s' in the record, %v", existingBin.Name, err)
				}
			default:
				return status, errors.Errorf("unknown update type '%s'", request.UpdateType.String())
			}
		}
	}

	record.Attributes = preparingAttributes
	record.Tags = preparingTags
	record.Bins = preparingBins
	record.UpdatedAt = time.Now().Unix()

	status.Updated = true
	status.Id = request.PrimaryKey
	return status, t.RecordStore.Set(ctx).ByKey("%s:rec:%s", record.Tenant, record.PrimaryKey).Proto(record)

}

func (t *implRecordService) BinPut(ctx context.Context, request *recordbasepb.BinPutRequest) (status *raftpb.Status, err error) {

	request.BinName = strings.TrimSpace(request.BinName)
	status = new(raftpb.Status)

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	record := new(recordbasepb.RecordEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", request.Tenant, request.PrimaryKey).ToProto(record)
	if err != nil {
		return
	}

	if record.PrimaryKey == "" || record.DeletedAt > 0 {
		return status, ErrNotFound
	}

	for _, bin := range record.Bins {
		if bin.Name == request.BinName {
			bin.Value = request.Value
			status.Updated = true
			break
		}
	}

	if !status.Updated {
		record.Bins = append(record.Bins, &recordbasepb.BinEntry{
			Name:  request.BinName,
			Value: request.Value,
		})
		status.Updated = true
	}

	return status, t.RecordStore.Set(ctx).ByKey("%s:rec:%s", record.Tenant, record.PrimaryKey).Proto(record)

}

func (t *implRecordService) BinRemove(ctx context.Context, request *recordbasepb.BinRemoveRequest) (status *raftpb.Status, err error) {

	request.BinName = strings.TrimSpace(request.BinName)
	status = new(raftpb.Status)

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	record := new(recordbasepb.RecordEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", request.Tenant, request.PrimaryKey).ToProto(record)
	if err != nil {
		return
	}

	if record.PrimaryKey == "" || record.DeletedAt > 0 {
		return status, ErrNotFound
	}

	var preparingBins []*recordbasepb.BinEntry
	for _, bin := range record.Bins {
		if bin.Name != request.BinName {
			preparingBins = append(preparingBins, bin)
		}
	}

	if len(preparingBins) == len(record.Bins) {
		return status, nil
	}

	status.Updated = true
	return status, t.RecordStore.Set(ctx).ByKey("%s:rec:%s", record.Tenant, record.PrimaryKey).Proto(record)

}



func (t *implRecordService) DownloadFile(ctx context.Context, tenant, primaryKey, fileName string, cb func(*recordbasepb.FileContent) bool) error {

	itemKey := digest256(fileName)
	chunk := 0

	for {
		existingData, err := t.RecordStore.Get(ctx).ByKey("%s:file:%s:%s:%d", tenant, primaryKey, itemKey, chunk).ToBinary()
		if err != nil {
			return err
		}

		if existingData != nil {
			if !cb(&recordbasepb.FileContent{Data: existingData}) {
				return nil
			}
		}

		chunk++
	}

	return nil
}

func (t *implRecordService) MapGet(ctx context.Context, tenant, primaryKey, mapKey string) (entry *recordbasepb.MapEntry, err error) {

	entry = new(recordbasepb.MapEntry)
	err = t.RecordStore.Set(ctx).ByKey("%s:map:%s:%s", tenant, primaryKey, mapKey).Proto(entry)
	return
}

func (t *implRecordService) MapRange(ctx context.Context, tenant, primaryKey string, cb func(entry *recordbasepb.MapEntry) bool) error {

	return t.RecordStore.
		Enumerate(ctx).
		ByPrefix("%s:map:%s:", tenant, primaryKey).
		DoProto(func() proto.Message {
		    return new(recordbasepb.MapEntry)
	    },
	    func(entry *store.ProtoEntry) bool {
			if e, ok := entry.Value.(*recordbasepb.MapEntry); ok {
				return cb(e)
			}
			return true
	    })

}

func (t *implRecordService) DeleteRecord(ctx context.Context, request *recordbasepb.DeleteRequest) (status *raftpb.Status, err error) {

	status = new(raftpb.Status)

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	existing := new(recordbasepb.RecordEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", request.Tenant, request.PrimaryKey).ToProto(existing)
	if err != nil {
		return
	}

	if existing.PrimaryKey == "" {
		// user not found
		return status, ErrNotFound
	}

	if existing.DeletedAt > 0 {
		return status, nil
	}

	/**
	Release all indexes
	 */
	for _, attr := range existing.Attributes {
		err = t.unindexAttribute(ctx, existing.Tenant, existing.PrimaryKey, attr)
		if err != nil {
			return
		}
	}
	for _, tag := range existing.Tags {
		err = t.unindexTag(ctx, existing.Tenant, existing.PrimaryKey, tag)
		if err != nil {
			return
		}
	}
	for _, bin := range existing.Bins {
		// decrement counter
		err = t.incrementCounter(ctx, existing.Tenant, "bin", bin.Name, -1)
		if err != nil {
			return
		}
	}

	ttlSeconds := t.DeleteGracePeriod * 24 * 3600

	/**
	Touch all files
	 */
	for _, fileInfo := range existing.Files {
		fileKey := digest256(fileInfo.Name)
		err = t.RecordStore.Touch(ctx).ByKey("%s:file:%s:%s", existing.Tenant, existing.PrimaryKey, fileKey).WithTtl(ttlSeconds).Do()
		if err != nil {
			return
		}
	}

	existing.DeletedAt = time.Now().Unix()
	// store temporary object with all attributes and with ttl
	err = t.RecordStore.Set(ctx).ByKey("%s:del:%s", existing.Tenant, existing.PrimaryKey).WithTtl(ttlSeconds).Proto(existing)
	if err != nil {
		return
	}

	existing.Attributes = nil
	existing.Tags = nil
	existing.Bins = nil
	existing.Files = nil

	// keep some attributes and store permanently
	err = t.RecordStore.Set(ctx).ByKey("%s:rec:%s", existing.Tenant, existing.PrimaryKey).Proto(existing)
	if err != nil {
		return
	}

	status.Updated = true
	return
}

func (t *implRecordService) UploadFile(ctx context.Context, request *recordbasepb.UploadFileRequest) (status *raftpb.Status, err error) {

	status = new(raftpb.Status)

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	itemKey := digest256(request.FileName)
	if request.Data != nil {
		err = t.RecordStore.Set(ctx).ByKey("%s:file:%s:%s:%d", request.Tenant, request.PrimaryKey, itemKey, request.Chunk).Binary(request.Data)
	} else {
		err = t.RecordStore.Remove(ctx).ByKey("%s:file:%s:%s:%d", request.Tenant, request.PrimaryKey, itemKey, request.Chunk).Do()
	}

	if request.Last {

		chunk := request.Chunk + 1

		for {
			var existingData []byte
			existingData, err = t.RecordStore.Get(ctx).ByKey("%s:file:%s:%s:%d", request.Tenant, request.PrimaryKey, itemKey, chunk).ToBinary()
			if err != nil {
				return
			}

			if existingData != nil {
				err = t.RecordStore.Remove(ctx).ByKey("%s:file:%s:%s:%d", request.Tenant, request.PrimaryKey, itemKey, chunk).Do()
				if err != nil {
					return
				}
			}

			chunk++
		}

	}

	record := new(recordbasepb.RecordEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", request.Tenant, request.PrimaryKey).ToProto(record)
	if err != nil {
		return
	}

	if record.PrimaryKey != "" {

		var updatingFile *recordbasepb.FileEntry
		for _, entry := range record.Files {
			if entry.Name == request.FileName {
				updatingFile = entry
				break
			}
		}

		if updatingFile == nil || request.Chunk == 0 {

			createdAt := time.Now().Unix()
			if updatingFile != nil {
				createdAt = updatingFile.CreatedAt
			}

			updatingFile = &recordbasepb.FileEntry{
				Name:      request.FileName,
				Size:      int32(len(request.Data)),
				CreatedAt: createdAt,
				UpdatedAt: time.Now().Unix(),
				DeletedAt: 0,
			}
			record.Files = append(record.Files, updatingFile)
		} else {
			updatingFile.Size += int32(len(request.Data))
			updatingFile.UpdatedAt = time.Now().Unix()
		}

		record.UpdatedAt = time.Now().Unix()
		err = t.RecordStore.Set(ctx).ByKey("%s:rec:%s", request.Tenant, request.PrimaryKey).Proto(record)
	}

	status.Updated = true
	return

}

func (t *implRecordService) DeleteFile(ctx context.Context, request *recordbasepb.DeleteFileRequest) (status *raftpb.Status, err error) {

	status = new(raftpb.Status)

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	itemKey := digest256(request.FileName)
	chunk := 0

	for {
		var existingData []byte
		existingData, err = t.RecordStore.Get(ctx).ByKey("%s:file:%s:%s:%d", request.Tenant, request.PrimaryKey, itemKey, chunk).ToBinary()
		if err != nil {
			return
		}

		if existingData != nil {
			err = t.RecordStore.Remove(ctx).ByKey("%s:file:%s:%s:%d", request.Tenant, request.PrimaryKey, itemKey, chunk).Do()
			if err != nil {
				return
			}
		}

		chunk++
	}

	record := new(recordbasepb.RecordEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", request.Tenant, request.PrimaryKey).ToProto(record)
	if err != nil {
		return
	}

	if record.PrimaryKey != "" {

		for _, entry := range record.Files {
			if entry.Name == request.FileName {
				entry.DeletedAt = time.Now().Unix()
			}
		}

		record.UpdatedAt = time.Now().Unix()
		err = t.RecordStore.Set(ctx).ByKey("%s:rec:%s", request.Tenant, request.PrimaryKey).Proto(record)

	}
	
	status.Updated = true
	return

}

func (t *implRecordService) AddKeyRange(ctx context.Context, in *recordbasepb.KeyRange) (status *raftpb.Status, err error) {

	status = new(raftpb.Status)

	if in.LastKey < in.FirstKey {
		return status, errors.Errorf("invalid key range %s", in.String())
	}

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	entity := new(recordbasepb.KeyRangeEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:keyrange", in.Tenant).ToProto(entity)
	if err != nil {
		return
	}

	entity.Ranges = append(entity.Ranges, in)

	sort.Slice(entity.Ranges, func(i, j int) bool {
		return entity.Ranges[i].FirstKey < entity.Ranges[j].FirstKey
	})

	err = t.RecordStore.Set(ctx).ByKey("%s:keyrange", in.Tenant).Proto(entity)
	return

}

func (t *implRecordService) MapPut(ctx context.Context, req *recordbasepb.MapPutRequest) (status *raftpb.Status, err error) {
	return &raftpb.Status{
		Id: req.PrimaryKey,
		Updated: true,
	},
	t.RecordStore.Set(ctx).ByKey("%s:map:%s:%s", req.Tenant, req.PrimaryKey, req.MapKey).Binary(req.Value)
}

func (t *implRecordService) MapRemove(ctx context.Context, req *recordbasepb.MapRemoveRequest) (status *raftpb.Status, err error) {
	return &raftpb.Status{
		Id: req.PrimaryKey,
		Updated: true,
	},
	t.RecordStore.Remove(ctx).ByKey("%s:map:%s:%s", req.Tenant, req.PrimaryKey, req.MapKey).Do()
}


func (t *implRecordService) GetKeyCapacity(ctx context.Context, tenant string) (cap *recordbasepb.KeyCapacity, err error) {

	cap = new(recordbasepb.KeyCapacity)

	entity := new(recordbasepb.KeyRangeEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:keyrange", tenant).ToProto(entity)
	if err != nil {
		return
	}

	for _, r := range entity.Ranges {
		if r.LastKey >= r.FirstKey {
			cap.PendingKeys += r.LastKey - r.FirstKey + 1
		}
	}

	return
}

func mapAttributes(list []*recordbasepb.AttributeEntry) map[string]*recordbasepb.AttributeEntry {
	cache := make(map[string]*recordbasepb.AttributeEntry)
	for _, item := range list {
		cache[item.Name] = item
	}
	return cache
}

func mapTags(list []string) map[string]bool {
	cache := make(map[string]bool)
	for _, item := range list {
		cache[item] = true
	}
	return cache
}

func mapBins(list []*recordbasepb.BinEntry) map[string]bool {
	cache := make(map[string]bool)
	for _, item := range list {
		cache[item.Name] = true
	}
	return cache
}
