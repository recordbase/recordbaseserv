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
	"github.com/recordbase/recordpb"
	"github.com/keyvalstore/store"
	"strings"
)

func (t *implRecordService) GetInfo(ctx context.Context, tenant string) (resp *recordpb.Info, err error) {

	prefix := fmt.Sprintf("%s:cnt:", tenant)

	resp = new(recordpb.Info)
	err = t.RecordStore.
		Enumerate(ctx).
		ByPrefix(prefix).
		DoCounters(func(entry *store.CounterEntry) bool {
			groupAndName := strings.TrimPrefix(string(entry.Key), prefix)
			parts := strings.SplitN(groupAndName, ":", 1)
			if len(parts) == 2 {
				switch parts[0] {
				case "attr":
					resp.Attributes = append(resp.Attributes, &recordpb.CountEntry {
						Name:  parts[1],
						Count: int64(entry.Value),
					})
				case "tag":
					resp.Tags = append(resp.Tags, &recordpb.CountEntry {
						Name:  parts[1],
						Count: int64(entry.Value),
					})
				case "bin":
					resp.Bins = append(resp.Bins, &recordpb.CountEntry {
						Name:  parts[1],
						Count: int64(entry.Value),
					})
				}
			}
			return true
		})

	return
}

func (t *implRecordService) incrementCounter(ctx context.Context, tenant, group, variable string, delta int64) (err error) {
	_, err = t.RecordStore.Increment(ctx).ByKey("%s:cnt:%s:%s", tenant, group, variable).WithDelta(delta).Do()
	return err
}

