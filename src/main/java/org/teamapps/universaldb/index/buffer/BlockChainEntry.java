/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
 * ---
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb.index.buffer;

import org.agrona.concurrent.AtomicBuffer;

import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

public class BlockChainEntry {

	/**
	 * Format description:
	 * Single block (4 - 2048 entries):
	 * length: 4b
	 * blockCount == totalCount: 4b
	 * Chain block:
	 * length: 4b
	 * blockCount: 4b
	 * totalCount: 4b
	 * nextBlock: 8b
	 * lastBlock: 8b
	 */

	private static final int BLOCK_COUNT_OFFSET = 4;
	private static final int TOTAL_COUNT_OFFSET = 8;
	private static final int NEXT_BLOCK_OFFSET = 12;
	private static final int LAST_BLOCK_OFFSET = 20;

	private final long position;
	private final int offset;
	private final AtomicBuffer buffer;
	private final BlockChainType chainType;
	private final ByteOrder byteOrder;

	public BlockChainEntry(long position, int offset, AtomicBuffer buffer, BlockChainType chainType, ByteOrder byteOrder) {
		this.position = position;
		this.offset = offset;
		this.buffer = buffer;
		this.chainType = chainType;
		this.byteOrder = byteOrder;
	}

	public int getAvailableSpace() {
		return chainType.getItems() - getBlockCount();
	}

	public void clearEntry() {
		writeInt(offset + BLOCK_COUNT_OFFSET, 0);
		if (chainType.isChain()) {
			writeInt(offset + TOTAL_COUNT_OFFSET, 0);
			writeLong(offset + NEXT_BLOCK_OFFSET, 0);
			writeLong(offset + LAST_BLOCK_OFFSET, 0);
		}
		int pos = offset + chainType.getDataOffset();
		for (int i = 0; i < chainType.getItems(); i++) {
			writeInt(pos, 0);
			pos += 4;
		}
	}

	public void readBlockEntries(List<Integer> list) {
		int blockCount = getBlockCount();
		int pos = offset + chainType.getDataOffset();
		for (int i = 0; i < chainType.getItems(); i++) {
			int value = readInt(pos);
			if (value > 0) {
				list.add(value);
			}
			pos += 4;
		}
	}

	public boolean containsBlockEntry(int entry) {
		int pos = offset + chainType.getDataOffset();
		for (int i = 0; i < chainType.getItems(); i++) {
			int value = readInt(pos);
			if (value == entry) {
				return true;
			}
		}
		return false;
	}

	public boolean containsBlockEntry(BitSet bitSet) {
		int pos = offset + chainType.getDataOffset();
		for (int i = 0; i < chainType.getItems(); i++) {
			int value = readInt(pos);
			if (value > 0 && bitSet.get(value)) {
				return true;
			}
		}
		return false;
	}

	public int writeBlockEntries(int startListPos, int length, List<Integer> list) {
		int listPos = startListPos;
		int writtenEntries = 0;
		int pos = offset + chainType.getDataOffset();
		for (int i = 0; i < chainType.getItems(); i++) {
			if (readInt(pos) <= 0) {
				writeInt(pos, list.get(listPos));
				listPos++;
				writtenEntries++;
				if (writtenEntries == length) {
					break;
				}
			}
			pos += 4;
		}
		addBlockCount(writtenEntries);
		return writtenEntries;
	}

	public int removeBlockEntries(Set<Integer> valueSet) {
		int pos = offset + chainType.getDataOffset();
		int deletionCount = 0;
		for (int i = 0; i < chainType.getItems(); i++) {
			int value = readInt(pos);
			if (value > 0 && valueSet.contains(value)) {
				writeInt(pos, 0);
				deletionCount++;
			}
			pos += 4;
		}
		subtractBlockCount(deletionCount);
		return deletionCount;
	}

	private void writeInt(int pos, int value) {
		buffer.putInt(pos, value, byteOrder);
	}

	private void writeLong(int pos, long value) {
		buffer.putLong(pos, value, byteOrder);
	}

	private int readInt(int pos) {
		return buffer.getInt(pos, byteOrder);
	}

	private long readLong(int pos) {
		return buffer.getLong(pos, byteOrder);
	}

	public int getTotalCount() {
		if (chainType.isChain()) {
			return readInt(offset + TOTAL_COUNT_OFFSET);
		} else {
			return getBlockCount();
		}
	}

	public void writeTotalCount(int count) {
		if (chainType.isChain()) {
			writeInt(offset + TOTAL_COUNT_OFFSET, count);
		}
	}

	public void addTotalCount(int add) {
		if (chainType.isChain()) {
			int totalCount = getTotalCount();
			writeTotalCount(totalCount + add);
		}
	}

	public void subtractTotalCont(int subtract) {
		if (chainType.isChain()) {
			int totalCount = getTotalCount();
			writeTotalCount(totalCount - subtract);
		}
	}

	public int getBlockCount() {
		return readInt(offset + BLOCK_COUNT_OFFSET);
	}

	public void writeBlockCount(int count) {
		writeInt(offset + BLOCK_COUNT_OFFSET, count);
	}

	public void addBlockCount(int add) {
		int blockCount = getBlockCount();
		writeBlockCount(blockCount + add);
	}

	public void subtractBlockCount(int subtract) {
		int blockCount = getBlockCount();
		writeBlockCount(blockCount - subtract);
	}

	public long getNextBlockPosition() {
		if (chainType.isChain()) {
			return readLong(offset + NEXT_BLOCK_OFFSET);
		} else {
			return 0;
		}
	}

	public void writeNextBlockPosition(long position) {
		if (chainType.isChain()) {
			writeLong(offset + NEXT_BLOCK_OFFSET, position);
		}
	}

	public long getLastBlockPosition() {
		if (chainType.isChain()) {
			return readLong(offset + LAST_BLOCK_OFFSET);
		} else {
			return 0;
		}
	}

	public void writeLastBlockPosition(long position) {
		if (chainType.isChain()) {
			writeLong(offset + LAST_BLOCK_OFFSET, position);
		}
	}

	public long getPosition() {
		return position;
	}

	public int getOffset() {
		return offset;
	}

	public AtomicBuffer getBuffer() {
		return buffer;
	}

	public BlockChainType getChainType() {
		return chainType;
	}
}
