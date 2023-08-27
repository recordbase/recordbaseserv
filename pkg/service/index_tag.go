/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package service

import (
	"context"
	"github.com/recordbase/recordbasepb"
	"go.uber.org/zap"
)

func (t *implRecordService) indexTag(ctx context.Context, tenant, primaryKey, tag string) (err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	digest := digestTag(tag)

	existing := new(recordbasepb.TagEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:tag:%s:%s", tenant, digest, primaryKey).ToProto(existing)
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
	return t.RecordStore.Set(ctx).ByKey("%s:tag:%s:%s", tenant, digest, primaryKey).Proto(existing)
}

func (t *implRecordService) unindexTag(ctx context.Context, tenant, primaryKey, tag string) (err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	digest := digestTag(tag)

	existing := new(recordbasepb.TagEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:tag:%s:%s", tenant, digest, primaryKey).ToProto(existing)
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

	return t.RecordStore.Remove(ctx).ByKey("%s:tag:%s:%s", tenant, digest, primaryKey).Do()

}




