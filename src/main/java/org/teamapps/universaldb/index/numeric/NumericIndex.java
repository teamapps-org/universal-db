package org.teamapps.universaldb.index.numeric;

import java.util.BitSet;

public interface NumericIndex {

	BitSet filter(BitSet records, NumericFilter numericFilter);
}
