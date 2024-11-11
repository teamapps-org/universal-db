/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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
package org.teamapps.universaldb.index.reference.value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RecordReference {

	public static List<Integer> createRecordIdsList(List<RecordReference> references) {
		List<Integer> list = new ArrayList<>();
		for (RecordReference reference : references) {
			if (reference.getRecordId() > 0) {
				list.add(reference.getRecordId());
			}
		}
		return list;
	}

	private int recordId;
	private final int correlationId;

	public RecordReference(int recordId, int correlationId) {
		this.recordId = recordId;
		this.correlationId = correlationId;
	}

	public int getRecordId() {
		return recordId;
	}

	public int getCorrelationId() {
		return correlationId;
	}

	public void setRecordId(int recordId) {
		if (recordId == 0) {
			return;
		}
		this.recordId = recordId;
	}

	public void updateReference(Map<Integer, Integer> recordIdByCorrelationId) {
		if (recordId == 0) {
			recordId = recordIdByCorrelationId.get(correlationId);
		}
	}

	@Override
	public String toString() {
		if (recordId > 0) {
			return "" + recordId;
		} else {
			return "(" + recordId + ", " + correlationId + ")";
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RecordReference that = (RecordReference) o;
		if (recordId != 0 && recordId == that.getRecordId()) {
			return true;
		}
		return correlationId != 0 && correlationId == that.getCorrelationId();
	}

	@Override
	public int hashCode() {
		if (recordId != 0) {
			return recordId;
		} else {
			return correlationId;
		}
	}
}
