package org.teamapps.universaldb.index.buffer.index;

import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;

import java.io.File;
import java.util.BitSet;
import java.util.Set;
import java.util.stream.IntStream;

public class DoubleAtomicMappedIndex {

	private final PrimitiveEntryAtomicStore atomicStore;

	public DoubleAtomicMappedIndex(File path, String name) {
		atomicStore = new PrimitiveEntryAtomicStore(path, name);
	}

	public double getValue(int id) {
		return atomicStore.getDouble(id);
	}

	public void setValue(int id, double value) {
		atomicStore.setDouble(id, value);
	}

	public boolean isEmpty(int id) {
		return getValue(id) != 0;
	}

	public int getMaximumId() {
		return atomicStore.getMaximumId(8);
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

	public BitSet filterEquals(double value, BitSet bitSet) {
		return filterEquals(value, bitSet.stream());
	}

	public BitSet filterEquals(double value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getDouble(id) == value).forEach(result::set);
		return result;
	}

	public BitSet filterNotEquals(double value, BitSet bitSet) {
		return filterNotEquals(value, bitSet.stream());
	}

	public BitSet filterNotEquals(double value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getDouble(id) != value).forEach(result::set);
		return result;
	}

	public BitSet filterGreater(double value, BitSet bitSet) {
		return filterGreater(value, bitSet.stream());
	}

	public BitSet filterGreater(double value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getDouble(id) > value).forEach(result::set);
		return result;
	}

	public BitSet filterSmaller(double value, BitSet bitSet) {
		return filterSmaller(value, bitSet.stream());
	}

	public BitSet filterSmaller(double value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getDouble(id) < value).forEach(result::set);
		return result;
	}

	public BitSet filterBetween(double startValue, double endValue, BitSet bitSet) {
		return filterBetween(startValue, endValue, bitSet.stream());
	}

	public BitSet filterBetween(double startValue, double endValue, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> {
			double v = atomicStore.getDouble(id);
			return v > startValue && v < endValue;
		}).forEach(result::set);
		return result;
	}

	public BitSet filterContains(Set<Double> valueSet, BitSet bitSet) {
		return filterContains(valueSet, bitSet.stream());
	}

	public BitSet filterContains(Set<Double> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> valueSet.contains(atomicStore.getDouble(id))).forEach(result::set);
		return result;
	}

	public BitSet filterContainsNot(Set<Double> valueSet, BitSet bitSet) {
		return filterContainsNot(valueSet, bitSet.stream());
	}

	public BitSet filterContainsNot(Set<Double> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> !valueSet.contains(atomicStore.getDouble(id))).forEach(result::set);
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
