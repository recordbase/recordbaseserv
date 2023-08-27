/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package service

import (
	"github.com/recordbase/recordbasepb"
	"github.com/go-errors/errors"
	"strings"
)

func NormalizeAttributes(attributes []*recordbasepb.AttributeEntry) ([]*recordbasepb.AttributeEntry, error) {

	for _, attr := range attributes {
		attr.Name = strings.TrimSpace(attr.Name)
		attr.Value = strings.TrimSpace(attr.Value)
	}

	return attributes, nil
}

func NormalizeTags(tags []string) ([]string, error) {

	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
	}

	return tags, nil
}

func NormalizeBins(bins []*recordbasepb.BinEntry) ([]*recordbasepb.BinEntry, error) {

	for _, bin := range bins {
		bin.Name = strings.TrimSpace(bin.Name)
	}

	return bins, nil

}

func NormalizeFiles(files []*recordbasepb.FileEntry) ([]*recordbasepb.FileEntry, error) {

	for _, file := range files {
		file.Name = strings.TrimSpace(file.Name)
	}

	return files, nil

}

func NormalizePathField(field string, skipOnUnknown bool) (string, error) {

	var out strings.Builder

	for i, ch := range []byte(field) {

		if ch >= 'a' && ch <= 'z' {
			out.WriteByte(ch)
			continue
		}

		if ch >= 'A' && ch <= 'Z' {
			out.WriteByte(ch)
			continue
		}

		if ch >= '0' && ch <= '9' {
			out.WriteByte(ch)
			continue
		}

		// no space ' ' (because of trimming) and no path separator ':' allowed
		if ch == '-' || ch == '_' ||
			ch == '`' || ch == '~' || ch == '|' ||
			ch == '!' || ch == '@' || ch == '#' || ch == '$' || ch == '&' || ch == '*' || ch == '=' || ch == '+' ||
			ch == '{' || ch == '}' || ch == '(' || ch == ')' || ch == '[' || ch == ']' ||
			ch == ';' || ch == '<' || ch == '>' || ch == '?' {
			out.WriteByte(ch)
			continue
		}

		if skipOnUnknown {
			continue
		}

		return "", errors.Errorf("unknown character '%v' in string '%s' on position %d", ch, field, i)
	}

	return out.String(), nil
}