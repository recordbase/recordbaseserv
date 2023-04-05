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
	"github.com/codeallergy/sprintframework/pkg/util"
	"github.com/recordbase/recordpb"
)

func (t *implRecordService) allocateId(ctx context.Context, tenant string) (userId string, err error) {

	ctx = t.TransactionalManager.BeginTransaction(ctx, false)
	defer func() {
		err = t.TransactionalManager.EndTransaction(ctx, err)
	}()

	entity := new(recordpb.KeyRangeEntry)
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

		recordEntity := new(recordpb.RecordEntry)
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
