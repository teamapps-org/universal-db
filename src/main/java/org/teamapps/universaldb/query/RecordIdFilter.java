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

import java.util.BitSet;
import java.util.Collection;

public class RecordIdFilter implements Filter {

	private final BitSet filterBitset;
	private IndexPath indexPath = new IndexPath();

	public RecordIdFilter(BitSet filterBitset) {
		this.filterBitset = filterBitset;
	}

	public RecordIdFilter(Collection<Integer> ids) {
		filterBitset = new BitSet();
		ids.forEach(filterBitset::set);
	}

	@Override
	public BitSet filter(BitSet input) {
		BitSet localRecords = indexPath.calculatePathBitSet(input);
		BitSet result = localFilter(localRecords);
		return indexPath.calculateReversePath(result, input);
	}

	@Override
	public BitSet localFilter(BitSet localRecords) {
		localRecords.and(filterBitset);
		return localRecords;
	}

	@Override
	public IndexPath getPath() {
		return indexPath;
	}

	@Override
	public void prependPath(IndexPath path) {
		path.addPath(indexPath);
		indexPath = path;
	}

	@Override
	public String explain(int level) {
		StringBuilder sb = new StringBuilder();
		sb.append(getExplainTabs(level));
		sb.append("custom-filter");
		sb.append("\n");
		return sb.toString();
	}
}
