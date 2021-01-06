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
package org.teamapps.universaldb.index.translation;

import org.teamapps.universaldb.index.text.TextFilterType;

public class TranslatableTextFilter {


	public static TranslatableTextFilter emptyFilter(String language) {
		return new TranslatableTextFilter(TextFilterType.EMPTY, null, language);
	}

	public static TranslatableTextFilter notEmptyFilter(String language) {
		return new TranslatableTextFilter(TextFilterType.NOT_EMPTY, null, language);
	}

	public static TranslatableTextFilter textEqualsFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TEXT_EQUALS, value, language);
	}

	public static TranslatableTextFilter textNotEqualsFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TEXT_NOT_EQUALS, value, language);
	}

	public static TranslatableTextFilter textByteLengthGreaterFilter(int size, String language) {
		return new TranslatableTextFilter(TextFilterType.TEXT_BYTE_LENGTH_GREATER, "" + size, language);
	}

	public static TranslatableTextFilter textByteLengthSmallerFilter(int size, String language) {
		return new TranslatableTextFilter(TextFilterType.TEXT_BYTE_LENGTH_SMALLER, "" + size, language);
	}

	public static TranslatableTextFilter termEqualsFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TERM_EQUALS, value, language);
	}

	public static TranslatableTextFilter termNotEqualsFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TERM_NOT_EQUALS, value, language);
	}

	public static TranslatableTextFilter termStartsWithFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TERM_STARTS_WITH, value, language);
	}

	public static TranslatableTextFilter termStartsNotWithFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TERM_STARTS_NOT_WITH, value, language);
	}

	public static TranslatableTextFilter termSimilarFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TERM_SIMILAR, value, language);
	}

	public static TranslatableTextFilter termNotSimilarFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TERM_NOT_SIMILAR, value, language);
	}

	public static TranslatableTextFilter termContainsFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TERM_CONTAINS, value, language);
	}

	public static TranslatableTextFilter termContainsNotFilter(String value, String language) {
		return new TranslatableTextFilter(TextFilterType.TERM_CONTAINS_NOT, value, language);
	}

	private final TextFilterType filterType;
	private final String value;
	private final String language;

	protected TranslatableTextFilter(TextFilterType filterType, String value, String language) {
		this.filterType = filterType;
		this.value = value;
		this.language = language;
	}

	public TextFilterType getFilterType() {
		return filterType;
	}

	public String getValue() {
		return value;
	}

	public String getLanguage() {
		return language;
	}

	@Override
	public String toString() {
		return filterType + " [" + language + "]" + ":" + value;
	}
}
