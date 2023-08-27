/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package service

var Scan = []interface{}{
	RecordService(),
	RaftService(),
}

