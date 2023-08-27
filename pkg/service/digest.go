/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package service

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

func digestAttribute(name, value string) string {
	return digest256(fmt.Sprintf("%s:%s", name, value))
}

func digestTag(tag string) string {
	return digest256(tag)
}

func digest256(str string) string {
	hash := sha256.New()
	hash.Write([]byte(str))
	return base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}
