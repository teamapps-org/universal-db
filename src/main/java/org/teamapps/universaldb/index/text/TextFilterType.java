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

public enum TextFilterType {

	EMPTY,
	NOT_EMPTY,

	TEXT_EQUALS,
	TEXT_EQUALS_IGNORE_CASE,
	TEXT_NOT_EQUALS,
	TEXT_BYTE_LENGTH_GREATER,
	TEXT_BYTE_LENGTH_SMALLER,

	TERM_EQUALS,
	TERM_NOT_EQUALS,
	TERM_STARTS_WITH,
	TERM_STARTS_NOT_WITH,
	TERM_SIMILAR,
	TERM_NOT_SIMILAR,
	TERM_CONTAINS,
	TERM_CONTAINS_NOT,

	;

	public boolean isFullTextIndexExclusive() {
		switch (this) {
			case TERM_EQUALS:
			case TERM_NOT_EQUALS:
			case TERM_STARTS_WITH:
			case TERM_STARTS_NOT_WITH:
			case TERM_SIMILAR:
			case TERM_NOT_SIMILAR:
			case TERM_CONTAINS:
			case TERM_CONTAINS_NOT:
				return true;
			default:
				return false;

		}
	}

	public boolean containsFullTextPart() {
		switch (this) {
			case EMPTY:
			case NOT_EMPTY:
			case TEXT_BYTE_LENGTH_GREATER:
			case TEXT_BYTE_LENGTH_SMALLER:
				return false;
			default:
				return true;
		}
	}

	public boolean containsIndexPart() {
		switch (this) {
			case EMPTY:
			case NOT_EMPTY:
			case TEXT_EQUALS:
			case TEXT_EQUALS_IGNORE_CASE:
			case TEXT_NOT_EQUALS:
			case TEXT_BYTE_LENGTH_GREATER:
			case TEXT_BYTE_LENGTH_SMALLER:
				return true;
			default:
				return false;
		}
	}

}
