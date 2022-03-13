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
