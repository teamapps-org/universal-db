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
import java.util.stream.Collectors;

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
		return count == 0;
	}

	@Override
	public boolean contains(Object o) {
		return indexOf(o) >= 0;
	}

	@Override
	public Iterator<ENTITY> iterator() {
		return new EntityArrayIterator<>(entityBuilder, recordIds);
	}

	@Override
	public Object[] toArray() {
		List<ENTITY> list = new ArrayList<>();
		for (int id : recordIds) {
			list.add(entityBuilder.build(id));
		}
		return list.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		List<ENTITY> list = new ArrayList<>();
		for (int id : recordIds) {
			list.add(entityBuilder.build(id));
		}
		return list.toArray(a);
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
		for (Object o : c) {
			if (indexOf(o) < 0) {
				return false;
			}
		}
		return true;
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
		Entity entity = (Entity) o;
		int id = entity.getId();
		for (int i = 0; i < recordIds.length; i++) {
			if (recordIds[i] == id) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public int lastIndexOf(Object o) {
		return indexOf(o); //list may not contain the same entity twice
	}

	@Override
	public ListIterator<ENTITY> listIterator() {
		return new ArrayList<>(this).listIterator();
	}

	@Override
	public ListIterator<ENTITY> listIterator(int index) {
		return new ArrayList<>(this).listIterator(index);
	}

	@Override
	public List<ENTITY> subList(int fromIndex, int toIndex) {
		return stream().skip(fromIndex).limit(toIndex - fromIndex).collect(Collectors.toList());
	}
}
