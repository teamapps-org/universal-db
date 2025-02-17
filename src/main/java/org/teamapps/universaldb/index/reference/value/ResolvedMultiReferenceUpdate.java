/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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

import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ResolvedMultiReferenceUpdate {

	private final ResolvedMultiReferenceType type;
	private final List<Integer> setReferences;
	private final List<Integer> addReferences;
	private final List<Integer> removeReferences;

	public static ResolvedMultiReferenceUpdate createRemoveAllReferences() {
		return new ResolvedMultiReferenceUpdate(ResolvedMultiReferenceType.REMOVE_ALL_REFERENCES, null, null, null);
	}

	public static ResolvedMultiReferenceUpdate createSetReferences(List<Integer> setReferences) {
		return new ResolvedMultiReferenceUpdate(ResolvedMultiReferenceType.SET_REFERENCES, setReferences, null, null);
	}

	public static ResolvedMultiReferenceUpdate createAddRemoveReferences(List<Integer> addReferences, List<Integer> removeReferences) {
		return new ResolvedMultiReferenceUpdate(ResolvedMultiReferenceType.ADD_REMOVE_REFERENCES, null, addReferences, removeReferences);
	}

	public static ResolvedMultiReferenceUpdate createAddRemoveReferences(CyclicReferenceUpdate referenceUpdate) {
		List<Integer> addReferences = referenceUpdate.isRemoveReference() ? Collections.emptyList() : Collections.singletonList(referenceUpdate.getReferencedRecordId());
		List<Integer> removeReferences = referenceUpdate.isRemoveReference() ? Collections.singletonList(referenceUpdate.getReferencedRecordId()) : Collections.emptyList();
		return new ResolvedMultiReferenceUpdate(ResolvedMultiReferenceType.ADD_REMOVE_REFERENCES, null, addReferences, removeReferences);
	}


	private ResolvedMultiReferenceUpdate(ResolvedMultiReferenceType type, List<Integer> setReferences, List<Integer> addReferences, List<Integer> removeReferences) {
		this.type = type;
		this.setReferences = setReferences;
		this.addReferences = addReferences;
		this.removeReferences = removeReferences;
	}

	public ResolvedMultiReferenceUpdate(DataInputStream dis) throws IOException {
		this.type = ResolvedMultiReferenceType.getById(dis.readByte());
		if (type == ResolvedMultiReferenceType.SET_REFERENCES) {
			setReferences = new ArrayList<>();
			addReferences = null;
			removeReferences = null;
			int count = dis.readInt();
			for (int i = 0; i < count; i++) {
				setReferences.add(dis.readInt());
			}
		} else if (type == ResolvedMultiReferenceType.ADD_REMOVE_REFERENCES) {
			setReferences = null;
			addReferences = new ArrayList<>();
			removeReferences = new ArrayList<>();
			int countAdd = dis.readInt();
			for (int i = 0; i < countAdd; i++) {
				addReferences.add(dis.readInt());
			}
			int countRemove = dis.readInt();
			for (int i = 0; i < countRemove; i++) {
				removeReferences.add(dis.readInt());
			}
		} else {
			setReferences = null;
			addReferences = null;
			removeReferences = null;
		}
	}

	public void write(DataOutputStream dos) throws IOException {
		dos.writeByte(type.getId());
		if (type == ResolvedMultiReferenceType.SET_REFERENCES) {
			dos.writeInt(setReferences.size());
			for (Integer id : setReferences) {
				dos.writeInt(id);
			}
		} else if (type == ResolvedMultiReferenceType.ADD_REMOVE_REFERENCES) {
			dos.writeInt(addReferences.size());
			for (Integer id : addReferences) {
				dos.writeInt(id);
			}
			dos.writeInt(removeReferences.size());
			for (Integer id : removeReferences) {
				dos.writeInt(id);
			}
		}
	}

	public ResolvedMultiReferenceType getType() {
		return type;
	}

	public List<Integer> getSetReferences() {
		return setReferences;
	}

	public List<Integer> getAddReferences() {
		return addReferences;
	}

	public List<Integer> getRemoveReferences() {
		return removeReferences;
	}
}
