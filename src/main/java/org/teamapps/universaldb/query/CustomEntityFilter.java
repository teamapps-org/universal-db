package org.teamapps.universaldb.query;

import java.util.BitSet;
import java.util.function.Function;

public class CustomEntityFilter implements Filter{

	private final Function<Integer, Boolean> filterFunction;
	private IndexPath indexPath = new IndexPath();

	public CustomEntityFilter(Function<Integer, Boolean> filterFunction) {
		this.filterFunction = filterFunction;
	}

	@Override
	public BitSet filter(BitSet input) {
		BitSet localRecords = indexPath.calculatePathBitSet(input);
		BitSet result = localFilter(localRecords);
		return indexPath.calculateReversePath(result, input);
	}

	@Override
	public BitSet localFilter(BitSet localRecords) {
		BitSet result = new BitSet();
		for (int id = localRecords.nextSetBit(0); id >= 0; id = localRecords.nextSetBit(id + 1)) {
			if (filterFunction.apply(id)) {
				result.set(id);
			}
		}
		return result;
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
