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
package org.teamapps.universaldb.index.reference.blockindex.iterator;

import java.util.PrimitiveIterator;

public class DoubleValueIterator implements PrimitiveIterator.OfInt {

	private final int value1;
	private final int value2;
	private boolean first = true;
	private boolean next = true;

	public DoubleValueIterator(int value1, int value2) {
		this.value1 = value1;
		this.value2 = value2;
	}

	@Override
	public int nextInt() {
		if (first) {
			first = false;
			return value1;
		} else {
			next = false;
			return value2;
		}
	}

	@Override
	public boolean hasNext() {
		return next;
	}
}
