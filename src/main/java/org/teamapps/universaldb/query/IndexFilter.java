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
package org.teamapps.universaldb.query;

import org.teamapps.universaldb.index.FieldIndex;
import org.teamapps.universaldb.index.file.FileFilter;
import org.teamapps.universaldb.index.text.TextFieldFilter;
import org.teamapps.universaldb.index.text.TextFilter;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class IndexFilter<TYPE, FILTER> implements Filter {

	public static List<TextFieldFilter> createTextFilters(List<IndexFilter> filters) {
		List<TextFieldFilter> textFieldFilters = new ArrayList<>();
		for (IndexFilter filter : filters) {
			FieldIndex fieldIndex = filter.getColumnIndex();
			if (filter.getFilter() instanceof TextFilter) {
				TextFilter textFilter = (TextFilter) filter.getFilter();
				textFieldFilters.add(TextFieldFilter.create(textFilter, fieldIndex.getName()));
			} else if (filter.getFilter() instanceof FileFilter) {
				//currently file content indexing is performed with FileIndex
			}
		}
		return textFieldFilters;
	}

	private final FieldIndex<TYPE, FILTER> fieldIndex;
	private final FILTER filter;
	private final boolean fullTextFilter;
	private IndexPath indexPath;

	public IndexFilter(FieldIndex<TYPE, FILTER> fieldIndex, FILTER filter) {
		this(fieldIndex, filter, new IndexPath());
	}

	public IndexFilter(FieldIndex<TYPE, FILTER> fieldIndex, FILTER filter, IndexPath indexPath) {
		this.fieldIndex = fieldIndex;
		this.filter = filter;
		this.fullTextFilter = filter instanceof TextFilter || filter instanceof FileFilter;
		this.indexPath = indexPath;
	}

	public boolean isFullTextFilter() {
		return fullTextFilter;
	}

	public FieldIndex<TYPE, FILTER> getColumnIndex() {
		return fieldIndex;
	}

	public FILTER getFilter() {
		return filter;
	}

	@Override
	public BitSet filter(BitSet input) {
		BitSet localRecords = indexPath.calculatePathBitSet(input);
		BitSet result = localFilter(localRecords);
		return indexPath.calculateReversePath(result, input);
	}

	@Override
	public BitSet localFilter(BitSet localRecords) {
		return fieldIndex.filter(localRecords, filter);
	}

	@Override
	public IndexPath getPath() {
		return indexPath;
	}

	@Override
	public void prependPath(IndexPath path) {
		IndexPath copy = path.copy();
		if (indexPath == null) {
			indexPath = copy;
		} else {
			copy.addPath(indexPath);
			indexPath = copy;
		}
	}

	@Override
	public String explain(int level) {
		StringBuilder sb = new StringBuilder();
		sb.append(getExplainTabs(level));
		if (indexPath != null && !indexPath.isLocalPath()) {
			sb.append(indexPath).append(": ");
		}
		sb.append(fieldIndex.getFQN()).append(": ").append(filter);
		sb.append("\n");
		return sb.toString();
	}

	@Override
	public String toString() {
		return explain(0);
	}
}
