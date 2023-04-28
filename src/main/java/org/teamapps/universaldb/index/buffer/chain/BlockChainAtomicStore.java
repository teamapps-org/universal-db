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
package org.teamapps.universaldb.index.buffer.chain;

import org.agrona.concurrent.AtomicBuffer;
import org.teamapps.universaldb.index.buffer.common.AbstractBlockEntryAtomicStore;

import java.io.File;
import java.util.*;

public class BlockChainAtomicStore extends AbstractBlockEntryAtomicStore {

	public BlockChainAtomicStore(File path, String name) {
		super(path, name);
	}

	public int getEntryCount(int id) {
		BlockChainEntry block = getBlock(getBlockPosition(id));
		return block != null ? block.getTotalCount() : 0;
	}

	public boolean isEmpty(int id) {
		return getBlockPosition(id) == 0;
	}

	public List<Integer> getEntries(int id) {
		long position = getBlockPosition(id);
		BlockChainEntry startEntry = getBlock(position);
		if (startEntry != null) {
			List<Integer> list = new ArrayList<>();
			startEntry.readBlockEntries(list);
			BlockChainEntry chainEntry = startEntry;
			while ((chainEntry = getNextBlock(chainEntry)) != null) {
				chainEntry.readBlockEntries(list);
			}
			if (position != getBlockPosition(id)) {
				//chain has become invalid while reading - reloading entries
				return getEntries(id);
			}
			return list;
		}
		return Collections.emptyList();
	}

	public boolean containsEntry(int id, int entry) {
		long position = getBlockPosition(id);
		BlockChainEntry startEntry = getBlock(position);
		if (startEntry != null) {
			if (startEntry.containsBlockEntry(entry)) {
				if (position != getBlockPosition(id)) {
					return containsEntry(id, entry);
				}
				return true;
			}
			BlockChainEntry chainEntry = startEntry;
			while ((chainEntry = getNextBlock(chainEntry)) != null) {
				if (chainEntry.containsBlockEntry(entry)) {
					if (position != getBlockPosition(id)) {
						return containsEntry(id, entry);
					}
					return true;
				}
			}
			if (position != getBlockPosition(id)) {
				return containsEntry(id, entry);
			}
		}
		return false;
	}

	public boolean containsEntry(int id, BitSet bitSet) {
		long position = getBlockPosition(id);
		BlockChainEntry startEntry = getBlock(position);
		if (startEntry != null) {
			if (startEntry.containsBlockEntry(bitSet)) {
				if (position != getBlockPosition(id)) {
					return containsEntry(id, bitSet);
				}
				return true;
			}
			BlockChainEntry chainEntry = startEntry;
			while ((chainEntry = getNextBlock(chainEntry)) != null) {
				if (chainEntry.containsBlockEntry(bitSet)) {
					if (position != getBlockPosition(id)) {
						return containsEntry(id, bitSet);
					}
					return true;
				}
			}
			if (position != getBlockPosition(id)) {
				return containsEntry(id, bitSet);
			}
		}
		return false;
	}

	public int removeEntries(int id, List<Integer> entries) {
		if (entries == null || entries.isEmpty()) {
			return 0;
		}
		Set<Integer> removeSet = new HashSet<>(entries);
		long position = getBlockPosition(id);
		BlockChainEntry startEntry = getBlock(position);
		int removedEntryCount = 0;
		if (startEntry != null) {
			removedEntryCount += startEntry.removeBlockEntries(removeSet);
			BlockChainEntry chainEntry = startEntry;
			while ((chainEntry = getNextBlock(chainEntry)) != null) {
				removedEntryCount += chainEntry.removeBlockEntries(removeSet);
			}
			startEntry.subtractTotalCont(removedEntryCount);
			return removedEntryCount;
		}
		return 0;
	}

	public void removeEntry(int id, int value) {
		removeEntries(id, Collections.singletonList(value));
	}

	public void removeAllEntries(int id) {
		setEntries(id, null);
	}

	public void addEntries(int id, List<Integer> entries) {
		if (id <= 0 || entries == null || entries.isEmpty()) {
			return;
		}
		long position = getBlockPosition(id);
		if (position > 0) {
			BlockChainEntry startEntry = getBlock(position);
			if (!startEntry.getChainType().isChain() && startEntry.getAvailableSpace() < entries.size()) {
				List<Integer> allEntries = new ArrayList<>();
				startEntry.readBlockEntries(allEntries);
				allEntries.addAll(entries);
				setEntries(id, allEntries);
				return;
			}
			int length = Math.min(entries.size(), startEntry.getAvailableSpace());
			int writtenEntries = startEntry.writeBlockEntries(0, length, entries);
			BlockChainEntry previousEntry = startEntry;
			while (writtenEntries < entries.size()) {
				if (!previousEntry.getChainType().isChain()) {
					throw new RuntimeException("Error: try to write to chain that is a single block, id:" + id + ", position:" + position);
				}
				BlockChainEntry block;
				if (previousEntry.getNextBlockPosition() > 0) {
					block = getBlock(previousEntry.getNextBlockPosition());
				} else {
					block = createBlock(previousEntry.getChainType());
				}
				length = Math.min(entries.size() - writtenEntries, block.getAvailableSpace());
				if (length > 0) {
					writtenEntries += block.writeBlockEntries(writtenEntries, length, entries);
				}
				previousEntry.writeNextBlockPosition(block.getPosition());
				previousEntry = block;
			}
			startEntry.addTotalCount(entries.size());
		} else {
			setEntries(id, entries);
		}
	}

	public void addEntry(int id, int value) {
		addEntries(id, Collections.singletonList(value));
	}

	public void setEntries(int id, List<Integer> entries) {
		if (id <= 0) {
			return;
		}
		long removePosition = getBlockPosition(id);
		if (entries != null && !entries.isEmpty()) {
			BlockChainType chainType = BlockChainType.getTypeBySize(entries.size());
			BlockChainEntry newEntry = createBlock(chainType);
			int length = Math.min(entries.size(), chainType.getItems());
			int writtenEntries = newEntry.writeBlockEntries(0, length, entries);
			BlockChainEntry previousEntry = newEntry;
			while (writtenEntries < entries.size()) {
				BlockChainEntry chainEntry = createBlock(chainType);
				length = Math.min(entries.size() - writtenEntries, chainType.getItems());
				writtenEntries += chainEntry.writeBlockEntries(writtenEntries, length, entries);
				previousEntry.writeNextBlockPosition(chainEntry.getPosition());
				previousEntry = chainEntry;
			}
			newEntry.writeTotalCount(entries.size());
			setBlockPosition(id, newEntry.getPosition());
		} else {
			setBlockPosition(id, 0);
		}
		if (removePosition > 0) {
			while (removePosition > 0) {
				BlockChainEntry block = getBlock(removePosition);
				assert block != null;
				removePosition = block.getNextBlockPosition();
				block.clearEntry();
				removeBlock(block);
			}
		}
	}

	private BlockChainEntry getBlock(long position) {
		if (position <= 0) {
			return null;
		}
		int bufferIndex = getBufferIndex(position);
		int offset = getOffset(position, bufferIndex);
		AtomicBuffer atomicBuffer = getBuffer(bufferIndex);
		int length = atomicBuffer.getInt(offset, byteOrder);
		BlockChainType chainType = BlockChainType.getTypeByLength(length);
		return new BlockChainEntry(position, offset, atomicBuffer, chainType, byteOrder);
	}

	private BlockChainEntry getNextBlock(BlockChainEntry entry) {
		long position = entry.getNextBlockPosition();
		if (position > 0) {
			return getBlock(position);
		} else {
			return null;
		}
	}

	private BlockChainEntry createBlock(BlockChainType chainType) {
		int length = chainType.getBlockLength();
		Long freeSlot = getFreeSlot(length);
		if (freeSlot != null) {
			long position = freeSlot;
			int bufferIndex = getBufferIndex(position);
			int offset = getOffset(position, bufferIndex);
			AtomicBuffer atomicBuffer = getBuffer(bufferIndex);
			if (atomicBuffer.getInt(offset) != (-1 * length)) {
				throw new RuntimeException("Try to reuse deleted block entry that already exists, pos:" + position + ", index:" + this);
			}
			atomicBuffer.putInt(offset, length, byteOrder);
			return new BlockChainEntry(position, offset, atomicBuffer, chainType, byteOrder);
		} else {
			long position = findNextBlockPosition(getFreeSpacePosition(), length + 4);
			setFreeSpacePosition(position + length + 4);
			ensureCapacity(position + length + 4);
			int bufferIndex = getBufferIndex(position);
			int offset = getOffset(position, bufferIndex);
			AtomicBuffer atomicBuffer = getBuffer(bufferIndex);
			atomicBuffer.putInt(offset, length, byteOrder);
			return new BlockChainEntry(position, offset, atomicBuffer, chainType, byteOrder);
		}
	}

	private void removeBlock(BlockChainEntry entry) {
		entry.clearEntry();
		removeEntry(entry.getPosition());
	}


}
