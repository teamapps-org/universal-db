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
package org.teamapps.universaldb.query;

import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.file.FileIndex;
import org.teamapps.universaldb.index.text.TextFieldFilter;
import org.teamapps.universaldb.index.text.TextFilter;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.index.translation.TranslatableTextIndex;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class AndFilter implements Filter {

    private List<Filter> filters = new ArrayList<>();

    public AndFilter() {

    }

    public AndFilter(Filter filterA, Filter filterB) {
        filters.add(filterA);
        filters.add(filterB);
    }

    public Filter and(Filter filter) {
        if (filter != null) {
            filters.add(filter);
        }
        return this;
    }

    @Override
    public void prependPath(IndexPath path) {
        filters.forEach(filter -> filter.prependPath(path));
    }

    @Override
    public BitSet filter(BitSet input) {
        List<List<Filter>> mappedFilters = Filter.mapFiltersByPathAndExpense(filters);
        BitSet result = input;
        for (List<Filter> filters : mappedFilters) {
            IndexPath path = filters.get(0).getPath();
            if (path == null) {
                for (Filter filter : filters) {
                    result.and(filter.filter(result));
                }
            } else {
                BitSet localRecords = path.calculatePathBitSet(result);

                List<IndexFilter> collectionFullTextFilters = Filter.getCollectionFullTextFilters(filters);
                if (!collectionFullTextFilters.isEmpty()) {
                    List<TextFieldFilter> textFilters = IndexFilter.createTextFilters(collectionFullTextFilters);
                    TableIndex table = collectionFullTextFilters.get(0).getColumnIndex().getTable();
                    localRecords = table.getCollectionTextSearchIndex().filter(localRecords, textFilters, true);
                }

                for (Filter filter : Filter.getNonCollectionFullTextFilters(filters)) {
                    localRecords = filter.localFilter(localRecords);
                }

                for (IndexFilter filter : Filter.getCollectionFullTextFiltersWithLocalIndexFilterPart(filters)) {
                    ColumnIndex columnIndex = filter.getColumnIndex();
                    if (columnIndex instanceof TextIndex) {
                        TextIndex textIndex = (TextIndex) columnIndex;
                        localRecords = textIndex.filter(localRecords, (TextFilter) filter.getFilter(), false);
                    } else if (columnIndex instanceof TranslatableTextIndex) {
                        TranslatableTextIndex textIndex = (TranslatableTextIndex) columnIndex;
                        localRecords = textIndex.filter(localRecords, (TextFilter) filter.getFilter(), false);
                    } else if (columnIndex instanceof FileIndex) {
                        FileIndex fileIndex = (FileIndex) columnIndex;
                        //currently file index maintains its own index...
                    }
                }

                BitSet pathResult = path.calculateReversePath(localRecords, result);
                result.and(pathResult);
            }
        }
        return result;
    }

    @Override
    public BitSet localFilter(BitSet localRecords) {
        BitSet result = localRecords;
        for (Filter filter : filters) {
            result = filter.localFilter(result);
        }
        return result;
    }

    @Override
    public IndexPath getPath() {
        IndexPath lastPath = null;
        for (Filter filter : filters) {
            if (lastPath == null) {
                lastPath = filter.getPath();
            } else if (!lastPath.isSamePath(filter.getPath())) {
                return null;
            }
        }
        return lastPath;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    @Override
    public String explain(int level) {
        StringBuilder sb = new StringBuilder();
        sb.append(getExplainTabs(level)).append("AND (").append("\n");
        for (Filter filter : filters) {
            sb.append(filter.explain(level + 1));
        }
        sb.append(getExplainTabs(level)).append(")").append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return explain(0);
    }
}
