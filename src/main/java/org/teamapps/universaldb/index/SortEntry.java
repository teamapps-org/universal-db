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
package org.teamapps.universaldb.index;

import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.pojo.Entity;

import java.util.*;

public class SortEntry<ENTITY extends Entity> {

	private final int id;
	private final int leafId;
	private ENTITY entity;

	public SortEntry(int id) {
		this.id = id;
		this.leafId = id;
	}

	public SortEntry(int id, int leafId) {
		this.id = id;
		this.leafId = leafId;
	}

	public SortEntry(int id, int leafId, ENTITY entity) {
		this.id = id;
		this.leafId = leafId;
		this.entity = entity;
	}

	public int getId() {
		return id;
	}

	public int getLeafId() {
		return leafId;
	}

	public ENTITY getEntity() {
		return entity;
	}

	public void setEntity(ENTITY entity) {
		this.entity = entity;
	}

	public static <ENTITY extends Entity> List<SortEntry<ENTITY>> createSortEntries(List<ENTITY> entities, SingleReferenceIndex... path) {
		List<SortEntry<ENTITY>> entries = new ArrayList<>();
		boolean noPath = (path == null || path.length == 0);
		for (ENTITY entity : entities) {
			if (noPath) {
				entries.add(new SortEntry(entity.getId(), entity.getId(), entity));
			} else {
				int recordId = entity.getId();
				for (SingleReferenceIndex singleReferenceIndex : path) {
					recordId = singleReferenceIndex.getValue(recordId);
					if (recordId == 0) {
						break;
					}
				}
				entries.add(new SortEntry(entity.getId(), recordId, entity));
			}
		}
		return entries;
	}

	public static List<SortEntry> createSortEntries(BitSet records, SingleReferenceIndex... path) {
		List<SortEntry> entries = new ArrayList<>();
		boolean noPath = (path == null || path.length == 0);
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			if (noPath) {
				entries.add(new SortEntry(id));
			} else {
				int recordId = id;
				for (SingleReferenceIndex singleReferenceIndex : path) {
					recordId = singleReferenceIndex.getValue(recordId);
					if (recordId == 0) {
						break;
					}
				}
				entries.add(new SortEntry(id, recordId));
			}
		}
		return entries;
	}


}
