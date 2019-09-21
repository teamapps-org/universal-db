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
package org.teamapps.universaldb.index.reference.multi;

import org.teamapps.universaldb.pojo.Entity;

import java.util.*;
import java.util.stream.Collectors;

public class MultiReferenceFilter {

	private final MultiReferenceFilterType type;
	private int countFilter;
	private Set<Integer> referencesSet;

	public static MultiReferenceFilter createEqualsFilter(Collection<? extends Entity> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.EQUALS, 0, createIdSet(references));
	}

	public static MultiReferenceFilter createNotEqualsFilter(Collection<? extends Entity> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.NOT_EQUALS, 0, createIdSet(references));
	}

	public static MultiReferenceFilter createIsEmptyFilter() {
		return new MultiReferenceFilter(MultiReferenceFilterType.IS_EMPTY, 0, null);
	}

	public static MultiReferenceFilter createIsNotEmptyFilter() {
		return new MultiReferenceFilter(MultiReferenceFilterType.IS_NOT_EMPTY, 0, null);
	}

	public static MultiReferenceFilter createContainsAllFilter(Collection<? extends Entity> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.CONTAINS_ALL, 0, createIdSet(references));
	}

	public static MultiReferenceFilter createContainsAnyFilter(Collection<? extends Entity> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.CONTAINS_ANY, 0, createIdSet(references));
	}

	public static MultiReferenceFilter createContainsNotAnyFilter(Collection<? extends Entity> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.CONTAINS_ANY_NOT, 0, createIdSet(references));
	}

	public static MultiReferenceFilter createContainsNoneFilter(Collection<? extends Entity> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.CONTAINS_NONE, 0, createIdSet(references));
	}

	public static MultiReferenceFilter createContainsMoreThanEntriesFilter(int count) {
		return new MultiReferenceFilter(MultiReferenceFilterType.ENTRY_COUNT_GREATER, count, null);
	}

	public static MultiReferenceFilter createContainsLessThanEntriesFilter(int count) {
		return new MultiReferenceFilter(MultiReferenceFilterType.ENTRY_COUNT_LESSER, count, null);
	}

	public static MultiReferenceFilter createIdEqualsFilter(Collection<Integer> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.EQUALS, 0, new HashSet<>(references));
	}

	public static MultiReferenceFilter createIdNotEqualsFilter(Collection<Integer> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.NOT_EQUALS, 0, new HashSet<>(references));
	}

	public static MultiReferenceFilter createIdContainsAllFilter(Collection<Integer> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.CONTAINS_ALL, 0, new HashSet<>(references));
	}

	public static MultiReferenceFilter createIdContainsAnyFilter(Collection<Integer> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.CONTAINS_ANY, 0, new HashSet<>(references));
	}

	public static MultiReferenceFilter createIdContainsNotAnyFilter(Collection<Integer> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.CONTAINS_ANY_NOT, 0, new HashSet<>(references));
	}

	public static MultiReferenceFilter createIdContainsNoneFilter(Collection<Integer> references) {
		return new MultiReferenceFilter(MultiReferenceFilterType.CONTAINS_NONE, 0, new HashSet<>(references));
	}

	public static MultiReferenceFilter createFilter(MultiReferenceFilterType type, Collection<Integer> references) {
		return new MultiReferenceFilter(type, 0, new HashSet<>(references));
	}

	public static MultiReferenceFilter createCountFilter(MultiReferenceFilterType type, int count) {
		return new MultiReferenceFilter(type, count, null);
	}

	private static Set<Integer> createIdSet(Collection<? extends Entity> references) {
		if (references == null || references.isEmpty()) {
			return Collections.emptySet();
		} else {
			return references.stream().map(entity -> ((Entity) entity).getId()).collect(Collectors.toSet());
		}
	}

	protected MultiReferenceFilter(MultiReferenceFilterType type, int countFilter, Set<Integer> referencesSet) {
		this.type = type;
		this.countFilter = countFilter;
		this.referencesSet = referencesSet;
	}

	public MultiReferenceFilterType getType() {
		return type;
	}

	public int getCountFilter() {
		return countFilter;
	}

	public Set<Integer> getReferencesSet() {
		return referencesSet;
	}

	@Override
	public String toString() {
		return type + ":" + referencesSet;
	}
}
