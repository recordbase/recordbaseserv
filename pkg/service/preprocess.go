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
	"github.com/recordbase/recordpb"
	"github.com/go-errors/errors"
	"strings"
)

func NormalizeAttributes(attributes []*recordpb.AttributeEntry) ([]*recordpb.AttributeEntry, error) {

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

func NormalizeBins(bins []*recordpb.BinEntry) ([]*recordpb.BinEntry, error) {

	for _, bin := range bins {
		bin.Name = strings.TrimSpace(bin.Name)
	}

	return bins, nil

}

func NormalizeFiles(files []*recordpb.FileEntry) ([]*recordpb.FileEntry, error) {

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