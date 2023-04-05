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
	"bytes"
	"context"
	"fmt"
	"github.com/codeallergy/raftpb"
	"github.com/codeallergy/store"
	"github.com/recordbase/recordpb"
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

	RecordStore          store.ManagedDataStore     `inject:"bean=record-storage"`
	TransactionalManager store.TransactionalManager `inject:"bean=record-storage"`

	DeleteGracePeriod        int  `value:"record.delete-grace-period,default=45"`
	StripUnknownChars        bool `value:"record.strip-unknown-chars,default=false"`
	IgnoreDuplicateEntries   bool `value:"record.ignore-duplicate-entries,default=false"`
}

func RecordService() api.RecordService {
	return &implRecordService{}
}

func (t *implRecordService) GetRecord(ctx context.Context, tenant, primaryKey string, withFileContents bool) (entity *recordpb.RecordEntry, err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, true)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	entity = new(recordpb.RecordEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", tenant, primaryKey).ToProto(entity)

	if entity.PrimaryKey != primaryKey {
		return nil, ErrNotFound
	}

	if withFileContents {
		for _, fileInfo := range entity.Files {
			fileInfo.Data, err = t.RecordStore.Get(ctx).ByKey("%s:file:%s:%s", tenant, primaryKey, digest256(fileInfo.Name)).ToBinary()
			if err != nil {
				return
			}
		}
	}

	return
}

func (t *implRecordService) LookupRecord(ctx context.Context, tenant, attribute, key string, withFileContents bool) (entity *recordpb.RecordEntry, err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, true)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	digest := attributeDigest(attribute, key)

	var primaryKey string
	primaryKey, err = t.RecordStore.Get(ctx).ByKey("%s:attr:%s", tenant, digest).ToString()
	if err != nil {
		return
	}

	if primaryKey == "" {
		return nil, ErrNotFound
	}

	return t.GetRecord(ctx, tenant, primaryKey, withFileContents)
}

func (t *implRecordService) Search(ctx context.Context, tenant, attribute, key string, cb func(*recordpb.RecordEntry) bool) (err error) {

	digest := attributeDigest(attribute, key)

	prefix := fmt.Sprintf("%s:attr:%s:", tenant, digest)
	var processingErr error
	err = t.RecordStore.Enumerate(ctx).
		ByPrefix(prefix).
		Do(func(entry *store.RawEntry) bool {
			key := string(entry.Key)
			primaryKey := strings.TrimPrefix(key, prefix)
			entity := new(recordpb.RecordEntry)
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

func (t *implRecordService) Scan(ctx context.Context, tenant, prefix string, offset, limit int, cb func(*recordpb.RecordEntry) bool) error {

	if offset < 0 || limit <= 0 {
		return nil
	}

	return t.RecordStore.Enumerate(ctx).ByPrefix("%s:rec:%s", tenant, prefix).DoProto(func() proto.Message {
		return new(recordpb.RecordEntry)
	}, func(entry *store.ProtoEntry) bool {
		if offset > 0 {
			offset--
			return true
		}
		if limit > 0 {
			if value, ok := entry.Value.(*recordpb.RecordEntry); ok {
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


func (t *implRecordService) CreateRecord(ctx context.Context, request *recordpb.CreateRequest) (status *raftpb.Status, err error) {

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
		request.Attributes, request.Tags, request.Columns, request.Files)
	if err != nil {
		return
	}

	status.Updated = true
	return status, t.doCreateRecord(ctx, record)
}

func (t *implRecordService) newRecord(tenant, primaryKey string,
	attributes []*recordpb.AttributeEntry,
	tags []string,
	columns []*recordpb.ColumnEntry,
	files []*recordpb.FileEntry) (record *recordpb.RecordEntry, err error) {

	current := time.Now().Unix()

	attributes, err = NormalizeAttributes(attributes, t.StripUnknownChars)
	if err != nil {
		return
	}

	tags, err = NormalizeTags(tags, t.StripUnknownChars)
	if err != nil {
		return
	}

	columns, err = NormalizeColumns(columns, t.StripUnknownChars)
	if err != nil {
		return
	}

	files, err = NormalizeFiles(files, t.StripUnknownChars)
	if err != nil {
		return
	}

	record = &recordpb.RecordEntry{
		Tenant:     tenant,
		PrimaryKey: primaryKey,
		Attributes: attributes,
		Tags:       tags,
		Columns:    columns,
		Files:      files,
		CreatedAt:  current,
		UpdatedAt:  current,
	}

	return
}

func (t *implRecordService) doCreateRecord(ctx context.Context, entity *recordpb.RecordEntry) (err error) {

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

	return
}

func (t *implRecordService) UpdateRecord(ctx context.Context, request *recordpb.UpdateRequest) (status *raftpb.Status, err error) {

	status = new(raftpb.Status)

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	record := new(recordpb.RecordEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", request.Tenant, request.PrimaryKey).ToProto(record)
	if err != nil {
		return
	}

	if record.PrimaryKey == "" {

		// nothing to merge, run create flow

		var record *recordpb.RecordEntry
		record, err = t.newRecord(request.Tenant, request.PrimaryKey,
			request.Attributes, request.Tags, request.Columns, request.Files)
		if err != nil {
			return
		}

		status.Id = request.PrimaryKey
		status.Updated = true
		return status, t.doCreateRecord(ctx, record)
	}

	if record.DeletedAt > 0 {
		return status, ErrNotFound
	}

	/**
	Update Attributes
	 */

	var preparingAttributes []*recordpb.AttributeEntry
	existingAttributes := attrMap(record.Attributes)
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
			case recordpb.UpdateType_MERGE:
				// keep it
				preparingAttributes = append(preparingAttributes, existingAttribute)
			case recordpb.UpdateType_REPLACE:
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
	existingTags := tagMap(record.Tags)
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
			case recordpb.UpdateType_MERGE:
				// keep it
				preparingTags = append(preparingTags, existingTag)
			case recordpb.UpdateType_REPLACE:
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
	Update Columns
	*/

	var preparingColumns []*recordpb.ColumnEntry
	requestColumns := make(map[string]bool)

	for _, item := range request.Columns {
		if requestColumns[item.Name] {
			if t.IgnoreDuplicateEntries {
				continue
			} else {
				return nil, errors.Errorf("duplicate column '%s' in the record", item.Name)
			}
		}
		requestColumns[item.Name] = true
		preparingColumns = append(preparingColumns, item)
	}

	for _, existingColumn := range record.Columns {
		if !requestColumns[existingColumn.Name] {
			if request.UpdateType == recordpb.UpdateType_MERGE {
				// keep it
				preparingColumns = append(preparingColumns, existingColumn)
			}
		}
	}
	
	/**
	Update Files
	*/
	var preparingFiles []*recordpb.FileEntry
	existingFiles := filesMap(record.Files)
	requestFiles := make(map[string]bool)

	for _, item := range request.Files {
		if requestFiles[item.Name] {
			if t.IgnoreDuplicateEntries {
				continue
			} else {
				return nil, errors.Errorf("duplicate file '%s' in the record", item.Name)
			}
		}
		requestFiles[item.Name] = true

		itemKey := digest256(item.Name)
		if existingFile, ok := existingFiles[item.Name]; ok {
			// replace file value is different
			existingData, err := t.RecordStore.Get(ctx).ByKey("%s:file:%s:%s", record.Tenant, record.PrimaryKey, itemKey).ToBinary()
			if err != nil {
				return status, err
			}

			if !bytes.Equal(existingData, item.Data) {

				err := t.RecordStore.Set(ctx).ByKey("%s:file:%s:%s", record.Tenant, record.PrimaryKey, itemKey).Binary(item.Data)
				if err != nil {
					return status, err
				}

				existingFile.CreatedAt = time.Now().Unix()
			}

			preparingFiles = append(preparingFiles, existingFile)

		} else {
			// add file

			err := t.RecordStore.Set(ctx).ByKey("%s:file:%s:%s", record.Tenant, record.PrimaryKey, itemKey).Binary(item.Data)
			if err != nil {
				return status, err
			}

			item.Data = nil
			item.CreatedAt = time.Now().Unix()

			preparingFiles = append(preparingFiles, item)
		}
	}

	for _, existingFile := range record.Files {
		if !requestFiles[existingFile.Name] {

			switch request.UpdateType {
			case recordpb.UpdateType_MERGE:
				// keep it
				preparingFiles = append(preparingFiles, existingFile)
			case recordpb.UpdateType_REPLACE:
				// remove it
				err := t.RecordStore.Remove(ctx).ByKey("%s:file:%s:%s", record.Tenant, record.PrimaryKey, digest256(existingFile.Name)).Do()
				if err != nil {
					return status, err
				}

			default:
				return status, errors.Errorf("unknown update type '%s'", request.UpdateType.String())

			}

		}
	}

	record.Attributes = preparingAttributes
	record.Tags = preparingTags
	record.Columns = preparingColumns
	record.Files = preparingFiles
	record.UpdatedAt = time.Now().Unix()

	status.Updated = true
	status.Id = request.PrimaryKey
	return status, t.RecordStore.Set(ctx).ByKey("%s:rec:%s", record.Tenant, record.PrimaryKey).Proto(record)

}

func (t *implRecordService) DeleteRecord(ctx context.Context, request *recordpb.DeleteRequest) (status *raftpb.Status, err error) {

	status = new(raftpb.Status)

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	existing := new(recordpb.RecordEntry)
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
	existing.Columns = nil
	existing.Files = nil

	// keep some attributes and store permanently
	err = t.RecordStore.Set(ctx).ByKey("%s:rec:%s", existing.Tenant, existing.PrimaryKey).Proto(existing)
	if err != nil {
		return
	}

	status.Updated = true
	return
}

func (t *implRecordService) AddKeyRange(ctx context.Context, in *recordpb.KeyRange) (status *raftpb.Status, err error) {

	status = new(raftpb.Status)

	if in.LastKey < in.FirstKey {
		return status, errors.Errorf("invalid key range %s", in.String())
	}

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	entity := new(recordpb.KeyRangeEntry)
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

func (t *implRecordService) GetKeyCapacity(ctx context.Context, tenant string) (cap *recordpb.KeyCapacity, err error) {

	cap = new(recordpb.KeyCapacity)

	entity := new(recordpb.KeyRangeEntry)
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

func attrMap(list []*recordpb.AttributeEntry) map[string]*recordpb.AttributeEntry {
	cache := make(map[string]*recordpb.AttributeEntry)
	for _, item := range list {
		cache[item.Name] = item
	}
	return cache
}

func tagMap(list []string) map[string]bool {
	cache := make(map[string]bool)
	for _, item := range list {
		cache[item] = true
	}
	return cache
}

func filesMap(list []*recordpb.FileEntry) map[string]*recordpb.FileEntry {
	cache := make(map[string]*recordpb.FileEntry)
	for _, item := range list {
		cache[item.Name] = item
	}
	return cache
}

func indexStrings(list []string) map[string]bool {
	cache := make(map[string]bool)
	for _, name := range list {
		cache[name] = true
	}
	return cache
}
