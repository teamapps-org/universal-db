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
package org.teamapps.universaldb.index.text;

import org.teamapps.universaldb.index.translation.TranslatableText;

public class FullTextIndexValue {

	private final String fieldName;
	private final String value;
	private final TranslatableText translatableText;

	public FullTextIndexValue(String fieldName, String value) {
		this.fieldName = fieldName;
		if (value != null) {
			this.value = value;
		} else {
			this.value = "";
		}
		translatableText = null;
	}

	public FullTextIndexValue(String fieldName, TranslatableText translatableText) {
		this.fieldName = fieldName;
		this.value = null;
		this.translatableText = translatableText;
	}

	public String getFieldName() {
		return fieldName;
	}

	public String getValue() {
		return value;
	}

	public String getValueNonNull() {
		return value != null ? value : "";
	}

	public TranslatableText getTranslatableText() {
		return translatableText;
	}

	public boolean isTranslatableText() {
		return translatableText != null;
	}
}
