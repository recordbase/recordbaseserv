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

func NormalizeAttributes(attributes []*recordpb.AttributeEntry, skipOnUnknown bool) ([]*recordpb.AttributeEntry, error) {

	var out []*recordpb.AttributeEntry

	for _, attr := range attributes {

		if val, err := NormalizePathField(attr.Name, skipOnUnknown); err != nil {
			return nil, err
		} else if val != "" {
			attr.Name = val
			out = append(out, attr)
		}

	}

	return out, nil
}

func NormalizeTags(tags []string, skipOnUnknown bool) ([]string, error) {

	var out []string

	for _, tag := range tags {

		if val, err := NormalizePathField(tag, skipOnUnknown); err != nil {
			return nil, err
		} else if val != "" {
			out = append(out, tag)
		}

	}

	return out, nil
}

func NormalizeColumns(columns []*recordpb.ColumnEntry, skipOnUnknown bool) ([]*recordpb.ColumnEntry, error) {

	var out []*recordpb.ColumnEntry

	for _, column := range columns {

		if val, err := NormalizePathField(column.Name, skipOnUnknown); err != nil {
			return nil, err
		} else if val != "" {
			column.Name = val
			out = append(out, column)
		}

	}

	return out, nil

}

func NormalizeFiles(files []*recordpb.FileEntry, skipOnUnknown bool) ([]*recordpb.FileEntry, error) {

	var out []*recordpb.FileEntry

	for i, file := range files {

		newName := strings.TrimSpace(file.Name)

		if skipOnUnknown {
			if newName != "" {
				file.Name = newName
				out = append(out, file)
			}
		} else if newName != file.Name  {
			return nil, errors.Errorf("file name has spaces '%v' on position %d", file.Name, i)
		}

	}

	return out, nil

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