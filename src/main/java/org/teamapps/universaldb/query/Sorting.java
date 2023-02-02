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
package org.teamapps.universaldb.query;

public class Sorting {

	private final String sortFieldName;
	private final SortDirection sortDirection;
	private String[] sortFieldPath;

	public Sorting(String sortFieldName) {
		this.sortFieldName = sortFieldName;
		this.sortDirection = SortDirection.ASCENDING;
	}

	public Sorting(String sortFieldName, boolean ascending) {
		this.sortFieldName = sortFieldName;
		this.sortDirection = ascending == true ? SortDirection.ASCENDING : SortDirection.DESCENDING;
	}


	public Sorting(String sortFieldName, SortDirection sortDirection, String... sortFieldPath) {
		this.sortFieldName = sortFieldName;
		this.sortDirection = sortDirection;
		this.sortFieldPath = sortFieldPath;
	}

	public String getSortFieldName() {
		return sortFieldName;
	}

	public SortDirection getSortDirection() {
		return sortDirection;
	}

	public String[] getSortFieldPath() {
		return sortFieldPath;
	}
}
