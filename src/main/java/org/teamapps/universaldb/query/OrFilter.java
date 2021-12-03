/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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

public class OrFilter implements Filter {

    private List<Filter> filters = new ArrayList<>();

    public OrFilter() {

    }

    public OrFilter(Filter filter) {
        filters.add(filter);
    }

    public OrFilter(Filter filterA, Filter filterB) {
        filters.add(filterA);
        filters.add(filterB);
    }

    @Override
    public Filter or(Filter filter) {
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
        if (mappedFilters.isEmpty()) {
            return input;
        }
        BitSet result = null;
        for (List<Filter> filters : mappedFilters) {
            IndexPath path = filters.get(0).getPath();
            if (path == null) {
                for (Filter filter : filters) {
                    BitSet reduced = filter.filter(input);
                    if (result == null) {
                        result = reduced;
                    } else {
                        result.or(reduced);
                    }
                }
            } else {
                BitSet localRecords = path.calculatePathBitSet(input);
                BitSet localResult = null;


                List<IndexFilter> collectionFullTextFilters = Filter.getCollectionFullTextFilters(filters);
                if (!collectionFullTextFilters.isEmpty()) {
                    List<TextFieldFilter> textFilters = IndexFilter.createTextFilters(collectionFullTextFilters);
                    TableIndex table = collectionFullTextFilters.get(0).getColumnIndex().getTable();
                    BitSet reduced = table.getCollectionTextSearchIndex().filter(localRecords, textFilters, false);

                    List<IndexFilter> secondaryFilter = Filter.getCollectionFullTextFiltersWithLocalIndexFilterPart(filters);
                    if (!secondaryFilter.isEmpty()) {
                        BitSet fullTextResult = null;
                        for (IndexFilter filter : secondaryFilter) {
                            ColumnIndex columnIndex = filter.getColumnIndex();
                            if (columnIndex instanceof TextIndex) {
                                TextIndex textIndex = (TextIndex) columnIndex;
                                BitSet fullTextReduced = textIndex.filter(reduced, (TextFilter) filter.getFilter(), false);
                                if (fullTextResult == null) {
                                    fullTextResult = fullTextReduced;
                                } else {
                                    fullTextResult.or(fullTextReduced);
                                }
                            } else if (columnIndex instanceof TranslatableTextIndex) {
                                TranslatableTextIndex textIndex = (TranslatableTextIndex) columnIndex;
                                BitSet fullTextReduced = textIndex.filter(reduced, (TextFilter) filter.getFilter(), false);
                                if (fullTextResult == null) {
                                    fullTextResult = fullTextReduced;
                                } else {
                                    fullTextResult.or(fullTextReduced);
                                }
                            } else if (columnIndex instanceof FileIndex) {
                                FileIndex fileIndex = (FileIndex) columnIndex;
                                //currently file index maintains its own index...
                            }
                        }
                        localResult = fullTextResult;
                    } else {
                        localResult = reduced;
                    }
                }

                for (Filter filter : Filter.getNonCollectionFullTextFilters(filters)) {
                    BitSet reduced = filter.localFilter(localRecords);
                    if (localResult == null) {
                        localResult = reduced;
                    } else {
                        localResult.or(reduced);
                    }
                }

                BitSet pathResult = path.calculateReversePath(localResult, input);
                if (result == null) {
                    result = pathResult;
                } else {
                    result.or(pathResult);
                }
            }
        }
        return result;
    }

    @Override
    public BitSet localFilter(BitSet localRecords) {
        BitSet result = null;
        for (Filter filter : filters) {
            BitSet reduced = filter.localFilter(localRecords);
            if (result == null) {
                result = reduced;
            } else {
                result.or(reduced);
            }
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
        sb.append(getExplainTabs(level)).append("OR (").append("\n");
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
