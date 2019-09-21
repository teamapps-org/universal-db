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


import org.teamapps.universaldb.record.EntityBuilder;

import java.util.*;

public class EntityArrayList<ENTITY> implements List<ENTITY> {

	private final EntityBuilder<ENTITY> entityBuilder;
	private PrimitiveIterator.OfInt recordIdIterator;
	private final int count;
	private int[] recordIds;

	public EntityArrayList(EntityBuilder<ENTITY> entityBuilder, PrimitiveIterator.OfInt recordIdIterator, int count) {
		this.entityBuilder = entityBuilder;
		this.recordIdIterator = recordIdIterator;
		this.count = count;
		int pos = 0;
		recordIds = new int[count];
		while (recordIdIterator.hasNext()) {
			recordIds[pos] = recordIdIterator.nextInt();
			pos++;
		}
	}

	@Override
	public int size() {
		return count;
	}

	@Override
	public boolean isEmpty() {
		return count > 0;
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<ENTITY> iterator() {
		return new EntityArrayIterator<>(entityBuilder, recordIds);
	}

	@Override
	public Object[] toArray() {
		return new Object[0];
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return null;
	}

	@Override
	public boolean add(ENTITY entity) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends ENTITY> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(int index, Collection<? extends ENTITY> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ENTITY get(int index) {
		return entityBuilder.build(recordIds[index]);
	}

	@Override
	public ENTITY set(int index, ENTITY element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void add(int index, ENTITY element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ENTITY remove(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int indexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<ENTITY> listIterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<ENTITY> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<ENTITY> subList(int fromIndex, int toIndex) {
		return null;
	}
}
