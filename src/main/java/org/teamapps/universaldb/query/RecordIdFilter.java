package org.teamapps.universaldb.query;

import java.util.BitSet;
import java.util.Collection;

public class RecordIdFilter implements Filter {

	private final BitSet filterBitset;
	private IndexPath indexPath = new IndexPath();

	public RecordIdFilter(BitSet filterBitset) {
		this.filterBitset = filterBitset;
	}

	public RecordIdFilter(Collection<Integer> ids) {
		filterBitset = new BitSet();
		ids.forEach(filterBitset::set);
	}

	@Override
	public BitSet filter(BitSet input) {
		BitSet localRecords = indexPath.calculatePathBitSet(input);
		BitSet result = localFilter(localRecords);
		return indexPath.calculateReversePath(result, input);
	}

	@Override
	public BitSet localFilter(BitSet localRecords) {
		localRecords.and(filterBitset);
		return localRecords;
	}

	@Override
	public IndexPath getPath() {
		return indexPath;
	}

	@Override
	public void prependPath(IndexPath path) {
		path.addPath(indexPath);
		indexPath = path;
	}

	@Override
	public String explain(int level) {
		StringBuilder sb = new StringBuilder();
		sb.append(getExplainTabs(level));
		sb.append("custom-filter");
		sb.append("\n");
		return sb.toString();
	}
}
