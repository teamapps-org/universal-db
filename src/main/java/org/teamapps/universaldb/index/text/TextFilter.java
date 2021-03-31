/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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
package org.teamapps.universaldb.index.text;

import org.teamapps.universaldb.context.UserContext;

import java.util.Collections;
import java.util.List;

public class TextFilter {


	public static TextFilter emptyFilter() {
		return new TextFilter(TextFilterType.EMPTY, null);
	}

	public static TextFilter notEmptyFilter() {
		return new TextFilter(TextFilterType.NOT_EMPTY, null);
	}

	public static TextFilter textEqualsFilter(String value) {
		return new TextFilter(TextFilterType.TEXT_EQUALS, value);
	}

	public static TextFilter textEqualsIgnoreCaseFilter(String value) {
		return new TextFilter(TextFilterType.TEXT_EQUALS_IGNORE_CASE, value);
	}

	public static TextFilter textNotEqualsFilter(String value) {
		return new TextFilter(TextFilterType.TEXT_NOT_EQUALS, value);
	}

	public static TextFilter textByteLengthGreaterFilter(int size) {
		return new TextFilter(TextFilterType.TEXT_BYTE_LENGTH_GREATER, "" + size);
	}

	public static TextFilter textByteLengthSmallerFilter(int size) {
		return new TextFilter(TextFilterType.TEXT_BYTE_LENGTH_SMALLER, "" + size);
	}

	public static TextFilter termEqualsFilter(String value) {
		return new TextFilter(TextFilterType.TERM_EQUALS, value);
	}

	public static TextFilter termNotEqualsFilter(String value) {
		return new TextFilter(TextFilterType.TERM_NOT_EQUALS, value);
	}

	public static TextFilter termStartsWithFilter(String value) {
		return new TextFilter(TextFilterType.TERM_STARTS_WITH, value);
	}

	public static TextFilter termStartsNotWithFilter(String value) {
		return new TextFilter(TextFilterType.TERM_STARTS_NOT_WITH, value);
	}

	public static TextFilter termSimilarFilter(String value) {
		return new TextFilter(TextFilterType.TERM_SIMILAR, value);
	}

	public static TextFilter termNotSimilarFilter(String value) {
		return new TextFilter(TextFilterType.TERM_NOT_SIMILAR, value);
	}

	public static TextFilter termContainsFilter(String value) {
		return new TextFilter(TextFilterType.TERM_CONTAINS, value);
	}

	public static TextFilter termContainsNotFilter(String value) {
		return new TextFilter(TextFilterType.TERM_CONTAINS_NOT, value);
	}

	private final TextFilterType filterType;
	private final String value;
	private UserContext userContext;

	protected TextFilter(TextFilterType filterType, String value) {
		this(filterType, value, null);
	}

	protected TextFilter(TextFilterType filterType, String value, UserContext userContext) {
		this.filterType = filterType;
		this.value = value;
		this.userContext = userContext;
	}

	public TextFilterType getFilterType() {
		return filterType;
	}

	public String getValue() {
		return value;
	}

	public List<String> getRankedLanguages() {
		return userContext != null ? userContext.getRankedLanguages() : Collections.singletonList("en");
	}

	public UserContext getUserContext() {
		return userContext;
	}

	@Override
	public String toString() {
		return userContext == null ? filterType + ":" + value : filterType + ":" + value + " (" + String.join(",", userContext.getRankedLanguages()) + ")";
	}
}
