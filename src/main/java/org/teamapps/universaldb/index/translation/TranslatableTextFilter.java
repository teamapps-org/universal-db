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
 * See the License for the specific userContext governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb.index.translation;

import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.text.TextFilter;
import org.teamapps.universaldb.index.text.TextFilterType;

public class TranslatableTextFilter extends TextFilter {


	public static TranslatableTextFilter emptyFilter(UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.EMPTY, null, userContext);
	}

	public static TranslatableTextFilter notEmptyFilter(UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.NOT_EMPTY, null, userContext);
	}

	public static TranslatableTextFilter textEqualsFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TEXT_EQUALS, value, userContext);
	}

	public static TranslatableTextFilter textNotEqualsFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TEXT_NOT_EQUALS, value, userContext);
	}

	public static TranslatableTextFilter textByteLengthGreaterFilter(int size, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TEXT_BYTE_LENGTH_GREATER, "" + size, userContext);
	}

	public static TranslatableTextFilter textByteLengthSmallerFilter(int size, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TEXT_BYTE_LENGTH_SMALLER, "" + size, userContext);
	}

	public static TranslatableTextFilter termEqualsFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TERM_EQUALS, value, userContext);
	}

	public static TranslatableTextFilter termNotEqualsFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TERM_NOT_EQUALS, value, userContext);
	}

	public static TranslatableTextFilter termStartsWithFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TERM_STARTS_WITH, value, userContext);
	}

	public static TranslatableTextFilter termStartsNotWithFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TERM_STARTS_NOT_WITH, value, userContext);
	}

	public static TranslatableTextFilter termSimilarFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TERM_SIMILAR, value, userContext);
	}

	public static TranslatableTextFilter termNotSimilarFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TERM_NOT_SIMILAR, value, userContext);
	}

	public static TranslatableTextFilter termContainsFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TERM_CONTAINS, value, userContext);
	}

	public static TranslatableTextFilter termContainsNotFilter(String value, UserContext userContext) {
		return new TranslatableTextFilter(TextFilterType.TERM_CONTAINS_NOT, value, userContext);
	}


	protected TranslatableTextFilter(TextFilterType filterType, String value, UserContext userContext) {
		super(filterType, value, userContext);
	}

}
