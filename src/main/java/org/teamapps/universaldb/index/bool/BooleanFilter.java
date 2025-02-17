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
package org.teamapps.universaldb.index.bool;

public class BooleanFilter {

	public static BooleanFilter trueFilter() {
		return new BooleanFilter(true);
	}

	public static BooleanFilter falseFilter() {
		return new BooleanFilter(false);
	}

	public static BooleanFilter create(boolean filter) {
		return new BooleanFilter(filter);
	}

	private final boolean filter;

	protected BooleanFilter(boolean filter) {
		this.filter = filter;
	}

	public boolean getFilterValue() {
		return filter;
	}

	@Override
	public String toString() {
		return "" + filter;
	}
}
