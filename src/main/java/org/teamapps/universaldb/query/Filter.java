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
package org.teamapps.universaldb.query;

import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.text.TextFilter;
import org.teamapps.universaldb.index.text.TextIndex;

import java.util.*;
import java.util.stream.Collectors;

public interface Filter {

    static Map<IndexPath, List<Filter>> mapFilters(List<Filter> filters) {
        Map<IndexPath, List<Filter>> filtersByPath = new HashMap<>();
        filters.stream()
                .filter(filter -> filter.getPath() != null)
                .forEach(filter -> filtersByPath.computeIfAbsent(filter.getPath(), f -> new ArrayList<>()).add(filter));
        return filtersByPath;
    }

    static List<List<Filter>> mapFiltersByPathAndExpense(List<Filter> filters) {
        Map<IndexPath, List<Filter>> map = mapFilters(filters);
        List<IndexPath> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys);
        List<List<Filter>> mappedFilters = new ArrayList<>();
        for (IndexPath key : keys) {
            mappedFilters.add(map.get(key));
        }
        //add nested or/and filters that cannot get reduced to a single path
        List<Filter> filtersWithoutPath = filters.stream().filter(filter -> filter.getPath() == null).collect(Collectors.toList());
        if (filtersWithoutPath != null && !filtersWithoutPath.isEmpty()) {
            mappedFilters.add(filtersWithoutPath);
        }
        return mappedFilters;
    }

    static List<IndexFilter> getCollectionFullTextFilters(List<Filter> filters) {
        return filters.stream()
                .filter(filter -> filter.isCollectionFullTextFilter(false))
                .map(filter -> (IndexFilter) filter)
                .collect(Collectors.toList());
    }

    static List<Filter> getNonCollectionFullTextFilters(List<Filter> filters) {
        return filters.stream()
                .filter(filter -> !filter.isCollectionFullTextFilter(false))
                .collect(Collectors.toList());
    }

    static List<IndexFilter> getCollectionFullTextFiltersWithLocalIndexFilterPart(List<Filter> filters) {
        List<IndexFilter> collectionFullTextFilters = getCollectionFullTextFilters(filters);
        return collectionFullTextFilters.stream()
                .filter(filter -> !filter.isCollectionFullTextFilter(true))
                .collect(Collectors.toList());
    }

    BitSet filter(BitSet input);

    BitSet localFilter(BitSet localRecords);

    IndexPath getPath();

    void prependPath(IndexPath path);

    default Filter and(Filter filter) {
        if (filter == null) {
            return this;
        }
        return new AndFilter(this, filter);
    }

    default Filter or(Filter filter) {
        if (filter == null) {
            return this;
        }
        return new OrFilter(this, filter);
    }

    default Filter asOrFilter() {
        return new OrFilter(this);
    }

    default boolean isCollectionFullTextFilter(boolean checkExclusive) {
        if (this instanceof IndexFilter) {
            IndexFilter indexFilter = (IndexFilter) this;
            ColumnIndex columnIndex = indexFilter.getColumnIndex();
            if (columnIndex.getType() == IndexType.TEXT) {
                TextFilter textFilter = (TextFilter) indexFilter.getFilter();
                TextIndex textIndex = (TextIndex) columnIndex;
                if (checkExclusive) {
                    return textIndex.isFilteredExclusivelyByCollectionTextIndex(textFilter);
                } else {
                    return textIndex.isFilteredByCollectionTextIndex(textFilter);
                }
            }
        }
        return false;
    }

    String explain(int level);

    default String getExplainTabs(int tabs) {
        StringBuilder sb = new StringBuilder();
        sb.append("\t".repeat(tabs));
        return sb.toString();
    }

}
