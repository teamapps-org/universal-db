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
package org.teamapps.universaldb.index.reference.value;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MultiReferenceEditValue implements MultiReferenceValue {

	private boolean removeAll;
	private final List<RecordReference> addReferences = new ArrayList<>();
	private final List<RecordReference> removeReferences = new ArrayList<>();
	private final List<RecordReference> setReferences = new ArrayList<>();

	public void setRemoveAll() {
		this.removeAll = true;
		addReferences.clear();
		removeReferences.clear();
		setReferences.clear();
	}

	public void setReferences(List<RecordReference> references) {
		setReferences.clear();
		removeAll = false;
		addReferences.clear();
		removeReferences.clear();
		Set<RecordReference> referenceSet = new HashSet<>();
		references.forEach(reference -> {
			if (!referenceSet.contains(reference)) {
				referenceSet.add(reference);
				setReferences.add(reference);
			}
		});

	}

	public void addReferences(List<RecordReference> references) {
		if (!setReferences.isEmpty()) {
			Set<RecordReference> referenceSet = new HashSet<>(setReferences);
			references.forEach(reference -> {
				if (!referenceSet.contains(reference)) {
					setReferences.add(reference);
					referenceSet.add(reference);
				}
			});
		} else {
			Set<RecordReference> addSet = new HashSet<>(addReferences);
			references.forEach(reference -> {
				removeReferences.remove(reference);
				if (!addSet.contains(reference)) {
					addReferences.add(reference);
				}
			});
		}
	}

	public void removeReferences(List<RecordReference> references) {
		if (!setReferences.isEmpty()) {
			references.forEach(setReferences::remove);
		} else if (removeAll){
			references.forEach(addReferences::remove);
		} else {
			references.forEach(reference -> {
				if (addReferences.contains(reference)) {
					addReferences.remove(reference);
				} else {
					removeReferences.add(reference);
				}
			});
		}
	}

	public boolean isRemoveAll() {
		return removeAll;
	}

	public List<RecordReference> getAddReferences() {
		return addReferences;
	}

	public List<RecordReference> getRemoveReferences() {
		return removeReferences;
	}

	public List<RecordReference> getSetReferences() {
		return setReferences;
	}

	@Override
	public MultiReferenceValueType getType() {
		return MultiReferenceValueType.EDIT_VALUE;
	}

	@Override
	public void writeValues(DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeBoolean(removeAll);
		writeReferences(this.addReferences, dataOutputStream);
		writeReferences(this.removeReferences, dataOutputStream);
		writeReferences(this.setReferences, dataOutputStream);
	}

	public void writeReferences(List<RecordReference> references, DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(references.size());
		for (RecordReference reference : references) {
			dataOutputStream.writeInt(reference.getRecordId());
			dataOutputStream.writeInt(reference.getCorrelationId());
		}
	}

	@Override
	public void readValues(DataInputStream dataInputStream) throws IOException {
		removeAll = dataInputStream.readBoolean();
		addReferences.addAll(readReferences(dataInputStream));
		removeReferences.addAll(readReferences(dataInputStream));
		setReferences.addAll(readReferences(dataInputStream));
	}

	private List<RecordReference> readReferences(DataInputStream dataInputStream) throws IOException {
		List<RecordReference> references = new ArrayList<>();
		int len = dataInputStream.readInt();
		for (int i = 0; i < len; i++) {
			int recordId = dataInputStream.readInt();
			int correlationId = dataInputStream.readInt();
			references.add(new RecordReference(recordId, correlationId));
		}
		return references;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (removeAll) {
			sb.append("REMOVE ALL ENTRIES");
		}
		if (!removeReferences.isEmpty()) {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append("REMOVE (").append(removeReferences.stream().map(val -> "" + val).collect(Collectors.joining(", "))).append(")");
		}
		if (!addReferences.isEmpty()) {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append("ADD (").append(addReferences.stream().map(val -> "" + val).collect(Collectors.joining(", "))).append(")");
		}
		if (!setReferences.isEmpty()) {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append("SET (").append(setReferences.stream().map(val -> "" + val).collect(Collectors.joining(", "))).append(")");
		}
		return sb.toString();
	}
}
