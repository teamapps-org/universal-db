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
package org.teamapps.universaldb.pojo;

import org.teamapps.universaldb.query.Filter;
import org.teamapps.universaldb.query.IndexFilter;
import org.teamapps.universaldb.query.IndexPath;
import org.teamapps.universaldb.query.OrFilter;

import java.util.BitSet;

public class AbstractUdbQuery {

	private Filter filter;

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

	public void and(IndexFilter andFilter) {
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

}
