/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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

import java.util.List;

public class TextFieldFilter extends TextFilter {

	private final String fieldName;
	private final boolean translatableField;

	public static TextFieldFilter create(TextFilter filter, String fieldName) { //todo translatableField
		return new TextFieldFilter(filter.getFilterType(), fieldName, filter.getUserContext() != null, filter.getUserContext(), filter.getValue());
	}

	public TextFieldFilter(TextFilterType filterType, String fieldName, String value) {
		this(filterType, fieldName, false, null, value);
	}

	public TextFieldFilter(TextFilterType filterType, String fieldName, boolean translatableField, UserContext userContext, String value) {
		super(filterType, value, userContext);
		this.fieldName = fieldName;
		this.translatableField = translatableField;
	}

	public String getFieldName() {
		return fieldName;
	}

	public boolean isTranslatableField() {
		return translatableField;
	}
}
