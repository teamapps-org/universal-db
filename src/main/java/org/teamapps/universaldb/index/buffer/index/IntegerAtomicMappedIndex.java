package org.teamapps.universaldb.index.buffer.index;

import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;

import java.io.File;
import java.util.BitSet;
import java.util.Set;
import java.util.stream.IntStream;

public class IntegerAtomicMappedIndex {

	private final PrimitiveEntryAtomicStore atomicStore;

	public IntegerAtomicMappedIndex(File path, String name) {
		atomicStore = new PrimitiveEntryAtomicStore(path, name);
	}

	public int getValue(int id) {
		return atomicStore.getInt(id);
	}

	public void setValue(int id, int value) {
		atomicStore.setInt(id, value);
	}

	public boolean isEmpty(int id) {
		return getValue(id) != 0;
	}

	public int getMaximumId() {
		return atomicStore.getMaximumId(4);
	}

	public int getLastNonEmptyIndex() {
		int maximumId = getMaximumId();
		for (int i = maximumId; i > 0; i--) {
			if (!isEmpty(i)) {
				return i;
			}
		}
		return -1;
	}

	public IntStream getIndexStream() {
		return IntStream.range(1, getMaximumId() + 1);
	}

	public BitSet filterEquals(int value, BitSet bitSet) {
		return filterEquals(value, bitSet.stream());
	}

	public BitSet filterEquals(int value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getInt(id) == value).forEach(result::set);
		return result;
	}

	public BitSet filterNotEquals(int value, BitSet bitSet) {
		return filterNotEquals(value, bitSet.stream());
	}

	public BitSet filterNotEquals(int value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getInt(id) != value).forEach(result::set);
		return result;
	}

	public BitSet filterGreater(int value, BitSet bitSet) {
		return filterGreater(value, bitSet.stream());
	}

	public BitSet filterGreater(int value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getInt(id) > value).forEach(result::set);
		return result;
	}

	public BitSet filterSmaller(int value, BitSet bitSet) {
		return filterSmaller(value, bitSet.stream());
	}

	public BitSet filterSmaller(int value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getInt(id) < value).forEach(result::set);
		return result;
	}

	public BitSet filterBetween(int startValue, int endValue, BitSet bitSet) {
		return filterBetween(startValue, endValue, bitSet.stream());
	}

	public BitSet filterBetween(int startValue, int endValue, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> {
			int v = atomicStore.getInt(id);
			return v > startValue && v < endValue;
		}).forEach(result::set);
		return result;
	}

	public BitSet filterContains(Set<Integer> valueSet, BitSet bitSet) {
		return filterContains(valueSet, bitSet.stream());
	}

	public BitSet filterContains(Set<Integer> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> valueSet.contains(atomicStore.getInt(id))).forEach(result::set);
		return result;
	}

	public BitSet filterContainsNot(Set<Integer> valueSet, BitSet bitSet) {
		return filterContainsNot(valueSet, bitSet.stream());
	}

	public BitSet filterContainsNot(Set<Integer> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> !valueSet.contains(atomicStore.getInt(id))).forEach(result::set);
		return result;
	}

	public void flush() {
		atomicStore.flush();
	}

	public void close() {
		atomicStore.close();
	}

	public void drop() {
		atomicStore.drop();
	}
}
