package org.teamapps.universaldb.query;

public enum SortDirection {

	ASCENDING,
	DESCENDING

	;

	public boolean isAscending() {
		return this == ASCENDING;
	}

}
