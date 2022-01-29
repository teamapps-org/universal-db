/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2022 TeamApps.org
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
package org.teamapps.universaldb.pojo;

import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.bool.BooleanFilter;
import org.teamapps.universaldb.index.bool.BooleanIndex;
import org.teamapps.universaldb.index.numeric.NumericFilter;
import org.teamapps.universaldb.index.text.TextFilter;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.query.*;
import org.teamapps.universaldb.record.EntityBuilder;

import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

public class AbstractUdbQuery<ENTITY extends Entity<ENTITY>> {

	private final TableIndex tableIndex;
	private final EntityBuilder<ENTITY> entityBuilder;
	private Filter filter;

	public AbstractUdbQuery(TableIndex tableIndex, EntityBuilder<ENTITY> entityBuilder) {
		this.tableIndex = tableIndex;
		this.entityBuilder = entityBuilder;
	}

	public Filter getFilter() {
		return filter;
	}

	public void and(AbstractUdbQuery query) {
		Filter andFilter = query.getFilter();
		if (filter == null) {
			filter = andFilter;
		} else {
			filter = filter.and(andFilter);
		}
	}

	public void prependPath(IndexPath path) {
		filter.prependPath(path);
	}
	
	public void or(Filter orFilter) {
		if (filter == null) {
			filter = orFilter;
		} else {
			filter = filter.or(orFilter);
		}
	}

	public void and(Filter andFilter) {
		if (filter == null) {
			filter = andFilter;
		} else {
			filter = filter.and(andFilter);
		}
	}

	public void and(IndexFilter andFilter) {
		if (filter == null) {
			filter = andFilter;
		} else {
			filter = filter.and(andFilter);
		}
	}

	public void and(CustomEntityFilter andFilter) {
		if (filter == null) {
			filter = andFilter;
		} else {
			filter = filter.and(andFilter);
		}
	}

	public void or(IndexFilter orFilter) {
		if (filter == null) {
			filter = orFilter;
		} else {
			filter = filter.or(orFilter);
		}
	}

	public void andOr(AbstractUdbQuery ... orFilters) {
		OrFilter orFilter = new OrFilter();
		for (AbstractUdbQuery query : orFilters) {
			orFilter.or(query.getFilter());
		}
		if (filter == null) {
			filter = orFilter;
		} else {
			filter = filter.and(orFilter);
		}
	}

	public void andOr(IndexFilter ... orFilters) {
		OrFilter orFilter = new OrFilter();
		for (IndexFilter filter : orFilters) {
			orFilter.or(filter);
		}
		if (filter == null) {
			filter = orFilter;
		} else {
			filter = filter.and(orFilter);
		}
	}

	public BitSet filter(BitSet input) {
		if (filter == null) {
			return input;
		} else {
			return filter.filter(input);
		}
	}

	public void addFullTextFilter(TextFilter textFilter, String... fieldNames) {
		and(tableIndex.createFullTextFilter(textFilter, fieldNames));
	}

	public void addFullTextQuery(String query, String... fieldNames) {
		and(tableIndex.createFullTextFilter(query, fieldNames));
	}

	public void addTextFilter(String columnName, TextFilter filter) {
		TextIndex textIndex = (TextIndex) tableIndex.getColumnIndex(columnName);
		IndexFilter<String, TextFilter> indexFilter = textIndex.createFilter(filter);
		and(indexFilter);
	}

	public void addNumericFilter(String columnName, NumericFilter filter) {
		ColumnIndex columnIndex = tableIndex.getColumnIndex(columnName);
		IndexFilter indexFilter = columnIndex.createFilter(filter);
		and(indexFilter);
	}

	public void addBooleanFilter(String columnName, BooleanFilter booleanFilter) {
		BooleanIndex booleanIndex = (BooleanIndex) tableIndex.getColumnIndex(columnName);
		IndexFilter<Boolean, BooleanFilter> indexFilter = booleanIndex.createFilter(booleanFilter);
		and(indexFilter);
	}


	public List<ENTITY> execute() {
		BitSet result = filter(tableIndex.getRecordBitSet());
		return new EntityBitSetList<>(entityBuilder, result);
	}

	public List<ENTITY> execute(boolean deletedRecords) {
		return deletedRecords ? executeOnDeletedRecords() : execute();
	}

	public List<ENTITY> executeOnDeletedRecords() {
		if (!tableIndex.getTableConfig().keepDeleted()) {
			throw new RuntimeException("Query error: this table has no 'keep deleted' option set.");
		}
		BitSet result = filter(tableIndex.getDeletedRecordsBitSet());
		return new EntityBitSetList<>(entityBuilder, result);
	}

	public ENTITY executeExpectSingleton() {
		BitSet result = filter(tableIndex.getRecordBitSet());
		int id = result.nextSetBit(1);
		if (id < 0) {
			return null;
		} else {
			return entityBuilder.build(id);
		}
	}

	public BitSet executeToBitSet() {
		return filter(tableIndex.getRecordBitSet());
	}

	public List<ENTITY> execute(String sortFieldName, boolean ascending, UserContext userContext, String ... path) {
		BitSet result = filter(tableIndex.getRecordBitSet());
		return AbstractUdbEntity.sort(tableIndex, entityBuilder, result, sortFieldName, ascending, userContext, path);
	}

	public List<ENTITY> execute(boolean deletedRecords, String sortFieldName, boolean ascending, UserContext userContext, String ... path) {
		if (deletedRecords && !tableIndex.getTableConfig().keepDeleted()) {
			throw new RuntimeException("Query error: this table has no 'keep deleted' option set.");
		}
		BitSet recordBitSet = deletedRecords ? tableIndex.getDeletedRecordsBitSet() : tableIndex.getRecordBitSet();
		BitSet result = filter(recordBitSet);
		if (sortFieldName == null || sortFieldName.isBlank()) {
			return new EntityBitSetList<>(entityBuilder, result);
		} else {
			return AbstractUdbEntity.sort(tableIndex, entityBuilder, result, sortFieldName, ascending, userContext, path);
		}
	}

	public List<ENTITY> execute(int startIndex, int length, Sorting sorting, UserContext userContext) {
		if (sorting == null) {
			return execute().stream()
					.skip(startIndex)
					.limit(length)
					.collect(Collectors.toList());
		} else {
			return execute(sorting.getSortFieldName(), sorting.getSortDirection().isAscending(), userContext, sorting.getSortFieldPath()).stream()
					.skip(startIndex)
					.limit(length)
					.collect(Collectors.toList());
		}
	}


	public TableIndex getTableIndex() {
		return tableIndex;
	}

	public EntityBuilder<ENTITY> getEntityBuilder() {
		return entityBuilder;
	}

	@Override
	public String toString() {
		Filter filter = getFilter();
		if (filter == null) {
			return "EMPTY QUERY";
		} else {
			return filter.toString();
		}
	}
}
