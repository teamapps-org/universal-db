package org.teamapps.universaldb.query;

public class Sorting {

	private final String sortFieldName;
	private final SortDirection sortDirection;
	private String[] sortFieldPath;

	public Sorting(String sortFieldName) {
		this.sortFieldName = sortFieldName;
		this.sortDirection = SortDirection.ASCENDING;
	}

	public Sorting(String sortFieldName, boolean ascending) {
		this.sortFieldName = sortFieldName;
		this.sortDirection = ascending == true ? SortDirection.ASCENDING : SortDirection.DESCENDING;
	}


	public Sorting(String sortFieldName, SortDirection sortDirection, String... sortFieldPath) {
		this.sortFieldName = sortFieldName;
		this.sortDirection = sortDirection;
		this.sortFieldPath = sortFieldPath;
	}

	public String getSortFieldName() {
		return sortFieldName;
	}

	public SortDirection getSortDirection() {
		return sortDirection;
	}

	public String[] getSortFieldPath() {
		return sortFieldPath;
	}
}
