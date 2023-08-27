/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package service

import (
	"context"
	"fmt"
	"github.com/keyvalstore/store"
	"github.com/recordbase/recordbasepb"
	"go.uber.org/zap"
	"strings"
)

func (t *implRecordService) indexAttribute(ctx context.Context, tenant, primaryKey string, attr *recordbasepb.AttributeEntry) (err error) {

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

	existing := new(recordbasepb.AttributeEntry)
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

func (t *implRecordService) unindexAttribute(ctx context.Context, tenant, primaryKey string, attr *recordbasepb.AttributeEntry) (err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	digest := digestAttribute(attr.Name, attr.Value)

	existing := new(recordbasepb.AttributeEntry)
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

