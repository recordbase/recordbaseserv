/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package service

import (
	"context"
	"github.com/sprintframework/sprintframework/pkg/util"
	"github.com/recordbase/recordbasepb"
)

func (t *implRecordService) allocateId(ctx context.Context, tenant string) (userId string, err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	entity := new(recordbasepb.KeyRangeEntry)
	err = t.RecordStore.Get(ctx).ByKey("%s:keyrange", tenant).ToProto(entity)
	if err != nil {
		return
	}

	needsUpdate := false
	defer func() {
		if needsUpdate {
			err = t.RecordStore.Set(ctx).ByKey("%s:keyrange", tenant).Proto(entity)
		}
	}()

	for true {

		if len(entity.Ranges) == 0 {
			return "", ErrOutOfRange
		}

		r := entity.Ranges[0]

		if r.LastKey < r.FirstKey {
			entity.Ranges = entity.Ranges[1:]
			needsUpdate = true
			continue
		}

		userId = util.EncodeId(uint64(r.FirstKey))
		r.FirstKey++
		if r.LastKey < r.FirstKey {
			entity.Ranges = entity.Ranges[1:]
		}
		needsUpdate = true

		recordEntity := new(recordbasepb.RecordEntry)
		err = t.RecordStore.Get(ctx).ByKey("%s:rec:%s", tenant, userId).ToProto(recordEntity)
		if err != nil {
			return "", err
		}
		if recordEntity.PrimaryKey != "" {
			// this entry already occupied, skip
			continue
		}

		return
	}

	return "", ErrOutOfRange
}
