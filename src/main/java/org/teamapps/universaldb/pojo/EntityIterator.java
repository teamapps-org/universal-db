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
package org.teamapps.universaldb.pojo;

import org.teamapps.universaldb.record.EntityBuilder;

import java.util.BitSet;
import java.util.Iterator;

public class EntityIterator<ENTITY> implements Iterator<ENTITY> {

	private final EntityBuilder<ENTITY> entityBuilder;
	private final BitSet records;
	private int id;
	private int pos;

	public EntityIterator(EntityBuilder<ENTITY> entityBuilder, BitSet records) {
		this.entityBuilder = entityBuilder;
		this.records = records;
	}

	@Override
	public boolean hasNext() {
		id = records.nextSetBit(pos);
		pos = id + 1;
		return id > 0;
	}

	@Override
	public ENTITY next() {
		return entityBuilder.build(id);
	}
}
