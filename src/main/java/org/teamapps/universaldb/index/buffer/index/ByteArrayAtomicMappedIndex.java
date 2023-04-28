package org.teamapps.universaldb.index.buffer.index;

import org.teamapps.universaldb.index.buffer.common.BlockEntryAtomicStore;

import java.io.File;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;
import java.util.stream.IntStream;

public class ByteArrayAtomicMappedIndex {

	private BlockEntryAtomicStore atomicStore;

	public ByteArrayAtomicMappedIndex(File path, String name) {
		this.atomicStore = new BlockEntryAtomicStore(path, name);
	}

	public byte[] getValue(int id) {
		return atomicStore.getBytes(id);
	}

	public void setValue(int id, byte[] value) {
		atomicStore.setBytes(id, value);
	}

	public void removeValue(int id) {
		atomicStore.removeText(id);
	}

	public boolean isEmpty(int id) {
		return atomicStore.isEmpty(id);
	}

	public int getMaximumId() {
		return atomicStore.getLastNonEmptyId();
	}

	public int getLastNonEmptyId() {
		return atomicStore.getLastNonEmptyId();
	}

	public IntStream getIndexStream() {
		return IntStream.range(1, getMaximumId() + 1);
	}

	public BitSet filterEquals(byte[] value, BitSet bitSet) {
		return filterEquals(value, bitSet.stream());
	}

	public BitSet filterEquals(byte[] value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> Arrays.equals(atomicStore.getBytes(id), value)).forEach(result::set);
		return result;
	}

	public BitSet filterNotEquals(byte[] value, BitSet bitSet) {
		return filterNotEquals(value, bitSet.stream());
	}

	public BitSet filterNotEquals(byte[] value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> !Arrays.equals(atomicStore.getBytes(id),value)).forEach(result::set);
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
