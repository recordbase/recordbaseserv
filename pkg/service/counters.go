/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package service

import (
	"context"
	"fmt"
	"github.com/recordbase/recordbasepb"
	"github.com/keyvalstore/store"
	"strings"
)

func (t *implRecordService) GetInfo(ctx context.Context, tenant string) (resp *recordbasepb.Info, err error) {

	prefix := fmt.Sprintf("%s:cnt:", tenant)

	resp = new(recordbasepb.Info)
	err = t.RecordStore.
		Enumerate(ctx).
		ByPrefix(prefix).
		DoCounters(func(entry *store.CounterEntry) bool {
			groupAndName := strings.TrimPrefix(string(entry.Key), prefix)
			parts := strings.SplitN(groupAndName, ":", 1)
			if len(parts) == 2 {
				switch parts[0] {
				case "attr":
					resp.Attributes = append(resp.Attributes, &recordbasepb.CountEntry {
						Name:  parts[1],
						Count: int64(entry.Value),
					})
				case "tag":
					resp.Tags = append(resp.Tags, &recordbasepb.CountEntry {
						Name:  parts[1],
						Count: int64(entry.Value),
					})
				case "bin":
					resp.Bins = append(resp.Bins, &recordbasepb.CountEntry {
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

