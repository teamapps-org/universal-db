package org.teamapps.universaldb.index.reference;

import org.teamapps.universaldb.index.TableIndex;

public interface ReferenceIndex {

	TableIndex getReferencedTable();

	boolean isCascadeDeleteReferences();
}
