/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
 * ---
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb.index.file;

import org.teamapps.universaldb.index.text.TextFieldFilter;
import org.teamapps.universaldb.index.text.TextFilterType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileFilter {

	public static FileFilter empty() {
		return sizeEquals(0);
	}

	public static FileFilter notEmpty() {
		return sizeGreater(0);
	}

	public static FileFilter termEquals(String value, FileDataField... fields) {
		return new FileFilter(FileFullTextFilterType.TERM_EQUALS, value, fields);
	}

	public static FileFilter termNotEquals(String value, FileDataField... fields) {
		return new FileFilter(FileFullTextFilterType.TERM_NOT_EQUALS, value, fields);
	}

	public static FileFilter termStartsWith(String value, FileDataField... fields) {
		return new FileFilter(FileFullTextFilterType.TERM_STARTS_WITH, value, fields);
	}

	public static FileFilter termStartNotWith(String value, FileDataField... fields) {
		return new FileFilter(FileFullTextFilterType.TERM_STARTS_NOT_WITH, value, fields);
	}

	public static FileFilter termSimilar(String value, FileDataField... fields) {
		return new FileFilter(FileFullTextFilterType.TERM_SIMILAR, value, fields);
	}

	public static FileFilter termNotSimilar(String value, FileDataField... fields) {
		return new FileFilter(FileFullTextFilterType.TERM_NOT_SIMILAR, value, fields);
	}

	public static FileFilter termContains(String value, FileDataField... fields) {
		return new FileFilter(FileFullTextFilterType.TERM_CONTAINS, value, fields);
	}

	public static FileFilter termContainsNot(String value, FileDataField... fields) {
		return new FileFilter(FileFullTextFilterType.TERM_CONTAINS_NOT, value, fields);
	}

	public static FileFilter sizeEquals(long size) {
		return new FileFilter(size, FileFilterType.SIZE_EQUALS);
	}

	public static FileFilter sizeNotEquals(long size) {
		return new FileFilter(size, FileFilterType.SIZE_NOT_EQUALS);
	}

	public static FileFilter sizeGreater(long size) {
		return new FileFilter(size, FileFilterType.SIZE_GREATER);
	}

	public static FileFilter sizeSmaller(long size) {
		return new FileFilter(size, FileFilterType.SIZE_SMALLER);
	}

	public static FileFilter sizeBetween(long size, long size2) {
		return new FileFilter(size, size2);
	}

	private FileFilterType filterType;
	private FileFullTextFilterType fullTextFilterType;
	private List<FileDataField> fields;

	private String value;
	private long size;
	private long size2;

	public FileFilter(FileFilterType filterType) {
		this.filterType = filterType;
	}

	protected FileFilter(FileFullTextFilterType fullTextFilterType, String value, FileDataField... fields) {
		this.filterType = FileFilterType.FULL_TEXT_FILTER;
		this.fullTextFilterType = fullTextFilterType;
		this.fields = Arrays.asList(fields);
		this.value = value;
		if (this.fields.isEmpty()) {
			this.fields = List.of(FileDataField.CONTENT, FileDataField.NAME);
		}
	}

	protected FileFilter(long size, FileFilterType filterType) {
		this.filterType = filterType;
		this.size = size;
	}

	protected FileFilter(long size, long size2) {
		this.filterType = FileFilterType.SIZE_BETWEEN;
		this.size = size;
		this.size2 = size2;
	}

	public List<TextFieldFilter> getTextFilters() {
		List<TextFieldFilter> textFieldFilters = new ArrayList<>();
		for (FileDataField field : fields) {
			String fieldName = field.name();
			TextFilterType textFilterType = getTextFilterType(fullTextFilterType);
			TextFieldFilter fieldFilter = new TextFieldFilter(textFilterType, fieldName, value);
			textFieldFilters.add(fieldFilter);
		}
		return textFieldFilters;
	}

	private TextFilterType getTextFilterType(FileFullTextFilterType fileFullTextFilterType) {
		switch (fileFullTextFilterType) {
			case TERM_EQUALS:
				return TextFilterType.TERM_EQUALS;
			case TERM_NOT_EQUALS:
				return TextFilterType.TERM_NOT_EQUALS;
			case TERM_STARTS_WITH:
				return TextFilterType.TERM_STARTS_WITH;
			case TERM_STARTS_NOT_WITH:
				return TextFilterType.TERM_STARTS_NOT_WITH;
			case TERM_SIMILAR:
				return TextFilterType.TERM_SIMILAR;
			case TERM_NOT_SIMILAR:
				return TextFilterType.TERM_NOT_SIMILAR;
			case TERM_CONTAINS:
				return TextFilterType.TERM_CONTAINS;
			case TERM_CONTAINS_NOT:
				return TextFilterType.TERM_CONTAINS_NOT;
		}
		return null;
	}


	public FileFilterType getFilterType() {
		return filterType;
	}

	public FileFullTextFilterType getFullTextFilterType() {
		return fullTextFilterType;
	}

	public List<FileDataField> getFields() {
		return fields;
	}

	public String getValue() {
		return value;
	}

	public long getSize() {
		return size;
	}

	public long getSize2() {
		return size2;
	}
}
