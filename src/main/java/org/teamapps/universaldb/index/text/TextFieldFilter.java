/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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

public class TextFieldFilter extends TextFilter {

	private final String fieldName;

	public static TextFieldFilter create(TextFilter filter, String fieldName) {
		return new TextFieldFilter(filter.getFilterType(), fieldName, filter.getValue());
	}

	public TextFieldFilter(TextFilterType filterType, String fieldName, String value) {
		super(filterType, value);
		this.fieldName = fieldName;
	}

	public String getFieldName() {
		return fieldName;
	}
}
