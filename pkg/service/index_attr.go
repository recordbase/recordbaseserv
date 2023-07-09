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
	"fmt"
	"github.com/keyvalstore/store"
	"github.com/recordbase/recordpb"
	"go.uber.org/zap"
	"strings"
)

func (t *implRecordService) indexAttribute(ctx context.Context, tenant, primaryKey string, attr *recordpb.AttributeEntry) (err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	digest := digestAttribute(attr.Name, attr.Value)

	// store primary index for lookup, not need to scan
	err = t.RecordStore.Set(ctx).ByKey("%s:attr:%s", tenant, digest).String(primaryKey)
	if err != nil {
		return
	}

	existing := new(recordpb.AttributeEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:attr:%s:%s", tenant, digest, primaryKey).ToProto(existing)
	if err != nil {
		return
	}

	if existing.Name == "" && existing.Value == "" {
		// not found, increment count

		err = t.incrementCounter(ctx, tenant, "attr", attr.Name, 1)
		if err != nil {
			return err
		}

	} else {

		// test on collision
		if existing.Name != attr.Name && existing.Value != attr.Value {
			t.Log.Error("HashCollision", zap.String("hash", digest), zap.String("existing", existing.String()), zap.String("new", attr.String()))
			return ErrCollision
		}

	}

	return t.RecordStore.Set(ctx).ByKey("%s:attr:%s:%s", tenant, digest, primaryKey).Proto(attr)
}

func (t *implRecordService) unindexAttribute(ctx context.Context, tenant, primaryKey string, attr *recordpb.AttributeEntry) (err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	digest := digestAttribute(attr.Name, attr.Value)

	existing := new(recordpb.AttributeEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:attr:%s:%s", tenant, digest, primaryKey).ToProto(existing)
	if err != nil {
		return
	}

	if existing.Name == "" && existing.Value == "" {
		// attribute not found
		return nil
	}

	// test on collision
	if existing.Name != attr.Name && existing.Value != attr.Value {
		t.Log.Error("HashCollision", zap.String("hash", digest), zap.String("existing", existing.String()), zap.String("new", attr.String()))
		return ErrCollision
	}

	err = t.incrementCounter(ctx, tenant, "attr", attr.Name, -1)
	if err != nil {
		return err
	}

	err = t.RecordStore.Remove(ctx).ByKey("%s:attr:%s:%s", tenant, digest, primaryKey).Do()
	if err != nil {
		return err
	}

	// get lookup index
	lookupPrimaryKey, err := t.RecordStore.Get(ctx).ByKey("%s:attr:%s", tenant, digest).ToString()
	if err != nil {
		return
	}

	if lookupPrimaryKey == primaryKey {

		var newPrimaryKey string
		prefix := fmt.Sprintf("%s:attr:%s:", tenant, digest)
		err = t.RecordStore.Enumerate(ctx).ByRawPrefix([]byte(prefix)).Do(func(entry *store.RawEntry) bool {
			nextPrimaryKey := strings.TrimPrefix(string(entry.Key), prefix)
			if nextPrimaryKey != primaryKey {
				newPrimaryKey = nextPrimaryKey
				return false
			}
			return true
		})

		if newPrimaryKey != "" {
			return t.RecordStore.Set(ctx).ByKey("%s:attr:%s", tenant, digest).String(newPrimaryKey)
		} else {
			return t.RecordStore.Remove(ctx).ByKey("%s:attr:%s", tenant, digest).Do()
		}

	}

	return nil

}

