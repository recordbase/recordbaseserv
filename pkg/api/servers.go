/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package api

import (
	"github.com/codeallergy/glue"
	"github.com/sprintframework/sprint"
	"reflect"
)

var GRPCServerClass = reflect.TypeOf((*GRPCServer)(nil)).Elem()

type GRPCServer interface {
	glue.InitializingBean
	sprint.Component
}

