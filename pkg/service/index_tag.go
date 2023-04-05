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
	"github.com/recordbase/recordpb"
	"go.uber.org/zap"
)

func (t *implRecordService) indexTag(ctx context.Context, tenant, primaryKey, tag string) (err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	existing := new(recordpb.TagEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:tag:%s:%s", tenant, tag, primaryKey).ToProto(existing)
	if err != nil {
		return
	}

	if existing.Tag == "" {
		// not found, increment count

		err = t.incrementCounter(ctx, tenant, "tag", tag, 1)
		if err != nil {
			return err
		}

	} else {

		// test on collision
		if existing.Tag != tag {
			t.Log.Error("TagCollision", zap.String("existing", existing.String()), zap.String("tag", tag))
			return ErrCollision
		}

	}

	existing.Tag = tag
	return t.RecordStore.Set(ctx).ByKey("%s:tag:%s:%s", tenant, tag, primaryKey).Proto(existing)
}

func (t *implRecordService) unindexTag(ctx context.Context, tenant, primaryKey, tag string) (err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	existing := new(recordpb.TagEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:tag:%s:%s", tenant, tag, primaryKey).ToProto(existing)
	if err != nil {
		return
	}

	if existing.Tag == "" {
		// tag not found
		return nil
	}

	// test on collision
	if existing.Tag != tag {
		t.Log.Error("TagCollision", zap.String("existing", existing.String()), zap.String("tag", tag))
		return ErrCollision
	}

	err = t.incrementCounter(ctx, tenant, "tag", tag, -1)
	if err != nil {
		return err
	}

	return t.RecordStore.Remove(ctx).ByKey("%s:tag:%s:%s", tenant, tag, primaryKey).Do()

}




