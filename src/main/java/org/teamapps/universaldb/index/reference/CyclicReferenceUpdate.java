/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2022 TeamApps.org
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
package org.teamapps.universaldb.index.reference;

import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;

public class CyclicReferenceUpdate {

	private final ColumnIndex referenceIndex;
	private final boolean removeReference;
	private final int recordId;
	private final int referencedRecordId;

	public CyclicReferenceUpdate(ColumnIndex referenceIndex, boolean removeReference, int recordId, int referencedRecordId) {
		this.referenceIndex = referenceIndex;
		this.removeReference = removeReference;
		this.recordId = recordId;
		this.referencedRecordId = referencedRecordId;
	}

	public boolean isSingleReference() {
		return referenceIndex.getType() == IndexType.REFERENCE;
	}

	public SingleReferenceIndex getSingleReferenceIndex() {
		return (SingleReferenceIndex) referenceIndex;
	}

	public MultiReferenceIndex getMultiReferenceIndex() {
		return (MultiReferenceIndex) referenceIndex;
	}

	public ColumnIndex getReferenceIndex() {
		return referenceIndex;
	}

	public boolean isRemoveReference() {
		return removeReference;
	}

	public int getRecordId() {
		return recordId;
	}

	public int getReferencedRecordId() {
		return referencedRecordId;
	}
}
