/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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
package org.teamapps.universaldb.index.reference.blockindex;

import org.teamapps.universaldb.util.MappedStoreUtil;
import org.teamapps.universaldb.index.reference.blockindex.iterator.ChainedBlockIterator;
import org.teamapps.universaldb.index.reference.blockindex.iterator.DoubleValueIterator;
import org.teamapps.universaldb.index.reference.blockindex.iterator.SingleBlockIterator;
import org.teamapps.universaldb.index.reference.blockindex.iterator.SingleValueIterator;

import java.io.File;
import java.util.*;
import java.util.PrimitiveIterator.OfInt;

public class ReferenceBlockChainImpl implements ReferenceBlockChain, ReferenceBlockProvider {

	private static final int MAX_DELETED_BLOCKS = 100_000;
	private ReferenceBuffer[] buffers;
	private ReferenceBuffer currentBuffer;
	private File path;
	private String name = "rel";
	private Map<Integer, Deque<DeletedBlock>> deletedBlocksByType;
	private ReferenceBlock writerBlock;

	public ReferenceBlockChainImpl(File path, String name) {
		this.path = path;
		if (name != null) {
			this.name = name;
		}
		deletedBlocksByType = new HashMap<>();
		for (BlockType value : BlockType.values()) {
			deletedBlocksByType.put(value.getId(), new ArrayDeque<>());
		}
		writerBlock = new ReferenceBlock(0, null, 0, null);
		init();
	}

	private void init() {
		int index = 0;
		do {
			addBuffer(index);
			index++;
		} while (getStoreFile(index).exists());
	}

	private ReferenceBuffer addBuffer() {
		return addBuffer(buffers.length);
	}

	private ReferenceBuffer addBuffer(int index) {
		ReferenceBuffer buffer = new ReferenceBuffer(getStoreFile(index), index);
		currentBuffer = buffer;
		if (index == 0) {
			buffers = new ReferenceBuffer[1];
			buffers[0] = buffer;
		} else {
			ReferenceBuffer[] newBuffers = new ReferenceBuffer[index + 1];
			System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
			newBuffers[buffers.length] = buffer;
			buffers = newBuffers;
		}
		return currentBuffer;
	}

	private File getStoreFile(int index) {
		return new File(path, name + "-" + index + ".rdx");
	}

	private ReferenceBlock reclaimOrCreateWriterBlock(BlockType blockType) {
		Deque<DeletedBlock> deletedBlocks = deletedBlocksByType.get(blockType.getId());
		if (deletedBlocks.size() > 0 && deletedBlocks.peekFirst().isAvailable(System.currentTimeMillis())) {
			DeletedBlock deletedBlock = deletedBlocks.poll();
			ReferenceBlock block = deletedBlock.getBlock();
			block.getBuffer().writeByte(blockType.getId(), block.getBlockPosition());
			return block;
		}
		if (currentBuffer.getRemainingSize() < blockType.getBlockSize()) {
			addBuffer();
		}
		currentBuffer.addBlock(writerBlock, blockType);
		return writerBlock;
	}

	private void deleteBlock(ReferenceBlock block) {
		block.getBuffer().writeByte(-1 * block.getBlockType().getId(), block.getBlockPosition());
		Deque<DeletedBlock> deletedBlocks = deletedBlocksByType.get(block.getBlockType().getId());
		if (deletedBlocks.size() < MAX_DELETED_BLOCKS) {
			deletedBlocks.offer(new DeletedBlock(block));
		}
	}

	@Override
	public long create(int value) {
		BlockType blockType = BlockType.SINGLE_ENTRY;
		ReferenceBlock block = reclaimOrCreateWriterBlock(blockType);
		block.getBuffer().writeInt(value, block.getBlockPosition() + 1);
		return block.getIndex();
	}

	@Override
	public long create(List<Integer> values) {
		BlockType blockType = BlockType.getEntryType(values.size());

		ReferenceBlock block = reclaimOrCreateWriterBlock(blockType);
		int len = Math.min(values.size(), blockType.getMaxEntries());
		Iterator<Integer> valueIterator = values.iterator();
		for (int i = 0; i < len; i++) {
			block.addValue(valueIterator.next(), i * 4);
		}
		block.writeBlockEntryCount(len);
		if (blockType == BlockType.ENTRIES_4096_START) {
			block.writeStartBlockTotalCount(values.size());
			block.writeStartBlockNextIndex(0);
			block.writeStartBlockLastIndex(0);
		}
		if (len < values.size()) {
			ReferenceBlock startBlock = addContinueBlocks(values, block, len, valueIterator);
			block = startBlock;
		}
		return block.getIndex();
	}

	public ReferenceBlock getBlock(long index) {
		if (index == 0) {
			return null;
		}
		ReferenceBuffer buffer = getBufferForIndex(index);
		return buffer.getBlock(index);
	}

	private void setBlockData(long index, ReferenceBlock block) {
		ReferenceBuffer buffer = getBufferForIndex(index);
		buffer.setBlockData(index, block);
	}

	@Override
	public long add(int value, long index) {
		setBlockData(index, writerBlock);
		if (writerBlock.addValueIfRemainingSpace(value)) {
			if (writerBlock.getBlockType() == BlockType.ENTRIES_4096_START) {
				writerBlock.writeStartBlockTotalCount(writerBlock.getBlockEntryCount());
			}
			return index;
		} else {
			if (writerBlock.getBlockType().isSingleBlock()) {
				ReferenceBlock deletionBlock = writerBlock.copy();
				ReferenceBlock block = reclaimOrCreateWriterBlock(writerBlock.getBlockType().getNextSize());
				block.copyContent(deletionBlock);
				int entries = deletionBlock.getBlockType().getMaxEntries();
				block.addValue(value, entries * 4);
				block.writeBlockEntryCount(entries + 1);
				if (block.getBlockType() == BlockType.ENTRIES_4096_START) {
					block.writeStartBlockTotalCount(entries + 1);
					block.writeStartBlockNextIndex(0);
					block.writeStartBlockLastIndex(0);
				}
				deleteBlock(deletionBlock);
				return block.getIndex();
			} else {
				ReferenceBlock startBlock = writerBlock.copy();
				startBlock.writeStartBlockTotalCount(startBlock.getTotalEntryCount() + 1);
				long lastIndex = writerBlock.getStartBlockLastIndex();
				if (lastIndex == 0) {
					writerBlock = reclaimOrCreateWriterBlock(BlockType.ENTRIES_4099_CONTINUE);
					writerBlock.addValue(value, 0);
					writerBlock.writeBlockEntryCount(1);
					startBlock.writeStartBlockNextIndex(writerBlock.getIndex());
					startBlock.writeStartBlockLastIndex(writerBlock.getIndex());
				} else {
					setBlockData(lastIndex, writerBlock);
					if (!writerBlock.addValueIfRemainingSpace(value)) {
						ReferenceBlock previousBlock = writerBlock.copy();
						writerBlock = reclaimOrCreateWriterBlock(BlockType.ENTRIES_4099_CONTINUE);
						previousBlock.writeNextIndex(writerBlock.getIndex());
						startBlock.writeStartBlockLastIndex(writerBlock.getIndex());
						writerBlock.addValue(value, 0);
						writerBlock.writeBlockEntryCount(1);
					}
				}
				return index;
			}
		}
	}

	@Override
	public long add(List<Integer> values, long index) {
		setBlockData(index, writerBlock);
		if (writerBlock.getBlockType().isSingleBlock()) {
			int remainingEntries = writerBlock.getRemainingEntries();
			if (remainingEntries >= values.size()) {
				int blockEntries = writerBlock.getBlockEntryCount();
				int offset = blockEntries * 4;
				for (int i = 0; i < values.size(); i++) {
					writerBlock.addValue(values.get(i), offset + i * 4);
				}
				writerBlock.writeBlockEntryCount(blockEntries + values.size());
				return index;
			} else {
				int blockEntries = writerBlock.getBlockEntryCount();
				ReferenceBlock deletionBlock = writerBlock.copy();
				BlockType blockType = writerBlock.getBlockType().getNextSize(values.size());

				ReferenceBlock block = reclaimOrCreateWriterBlock(blockType);
				block.writeStartBlockTotalCount(blockEntries);
				block.copyContent(deletionBlock, blockEntries);
				index = block.getIndex();
				addValuesToBlockChain(values, block, blockType, blockEntries);
				deleteBlock(deletionBlock);
				return index;
			}
		} else {
			ReferenceBlock block = writerBlock;
			BlockType blockType = block.getBlockType();
			assert blockType == BlockType.ENTRIES_4096_START;
			int blockEntries = block.getBlockEntryCount();

			addValuesToBlockChain(values, block, blockType, blockEntries);
			return index;

		}
	}

	private void addValuesToBlockChain(List<Integer> values, ReferenceBlock block, BlockType blockType, int blockEntries) {
		int len = Math.min(values.size(), blockType.getMaxEntries() - blockEntries);
		Iterator<Integer> valueIterator = values.iterator();
		int previousEntriesOffset = blockEntries * 4;
		for (int i = 0; i < len; i++) {
			block.addValue(valueIterator.next(), previousEntriesOffset + i * 4);
		}
		block.writeBlockEntryCount(len + blockEntries);

		if (blockType == BlockType.ENTRIES_4096_START) {
			block.writeStartBlockTotalCount(block.getTotalEntryCount() + values.size());
			block.writeStartBlockNextIndex(0);
			block.writeStartBlockLastIndex(0);
		}
		if (len < values.size()) {
			addContinueBlocks(values, block, len, valueIterator);
		}
	}

	private ReferenceBlock addContinueBlocks(List<Integer> values, ReferenceBlock block, int len, Iterator<Integer> valueIterator) {
		ReferenceBlock startBlock = block.copy();
		int pos = len;
		boolean firstContinueBlock = true;
		while (pos < values.size()) {
			ReferenceBlock previousBlock = block.copy();
			block = reclaimOrCreateWriterBlock(BlockType.ENTRIES_4099_CONTINUE);
			if (firstContinueBlock) {
				startBlock.writeStartBlockNextIndex(block.getIndex());
				firstContinueBlock = false;
			} else {
				previousBlock.writeNextIndex(block.getIndex());
			}
			len = Math.min(values.size() - pos, block.getBlockType().getMaxEntries());
			for (int i = 0; i < len; i++) {
				block.addValue(valueIterator.next(), i * 4);
			}
			block.writeBlockEntryCount(len);
			pos += len;
		}
		startBlock.writeStartBlockLastIndex(block.getIndex());
		return startBlock;
	}

	@Override
	public long remove(int deleteValue, long index) {
		return remove(Collections.singleton(deleteValue), index);
	}

	@Override
	public long remove(Set<Integer> deleteValues, long index) {
		ReferenceBlock block = getBlock(index);
		if (block.getBlockType().isSingleBlock()) {
			int dataPosition = block.getBlockDataPosition();
			int blockEntryCount = block.getBlockEntryCount();
			List<Integer> values = new ArrayList<>();
			for (int i = 0; i < blockEntryCount; i++) {
				int value = block.getBuffer().readInt(dataPosition + i * 4);
				if (!deleteValues.contains(value)) {
					values.add(value);
				}
			}
			if (blockEntryCount == values.size()) {
				return block.getIndex();
			} else if (values.isEmpty()) {
				deleteBlock(block);
				return 0;
			} else {
				//todo on remove single entry - remove full block? Better:
				BlockType blockType = BlockType.getEntryType(values.size());
				if (blockType == block.getBlockType()) {
					//would the reader check for deleted (negative) values?
					//todo adapt SingleBlockIterator to handle negative values
				} else {
					//remove and create new block with values -> see below
				}
				ReferenceBlock deleteBlock = block.copy();
				long newIndex = create(values);
				deleteBlock(deleteBlock);
				return newIndex;
			}
		} else {
			int countDeleted = 0;
			ReferenceBlock startBlock = block.copy();
			while (block.getNextIndex() != 0) {
				int dataPosition = block.getBlockDataPosition();
				int blockEntryCount = block.getBlockEntryCount();
				for (int i = 0; i < blockEntryCount; i++) {
					int value = block.getBuffer().readInt(dataPosition + i * 4);
					if (deleteValues.contains(value)) {
						block.getBuffer().writeInt(value * -1, dataPosition + i * 4);
						countDeleted++;
					}
				}
			}
			startBlock.writeStartBlockTotalCount(startBlock.getTotalEntryCount() - countDeleted);
			return index;
		}
	}

	@Override
	public void removeAll(long index) {
		ReferenceBlock block = getBlock(index);
		deleteBlock(block);
		if (!block.getBlockType().isSingleBlock()) {
			while (block.getNextIndex() != 0) {
				long nextIndex = block.getNextIndex();
				block = getBlock(nextIndex);
				deleteBlock(block);
			}
		}
	}

	@Override
	public OfInt getReferences(long index) {
		ReferenceBlock block = getBlock(index);
		BlockType blockType = block.getBlockType();
		switch (blockType) {
			case SINGLE_ENTRY:
				int value = block.getBuffer().readInt(block.getBlockDataPosition());
				return new SingleValueIterator(value);
			case ENTRIES_2:
				int value1 = block.getBuffer().readInt(block.getBlockDataPosition());
				int value2 = block.getBuffer().readInt(block.getBlockDataPosition() + 4);
				return new DoubleValueIterator(value1, value2);
			case ENTRIES_4096_START:
				return new ChainedBlockIterator(block, this);
			default:
				return new SingleBlockIterator(block);
		}
	}

	@Override
	public int getReferencesCount(long index) {
		ReferenceBlock block = getBlock(index);
		return block.getTotalEntryCount();
	}

	private ReferenceBuffer getBufferForIndex(long index) {
		return buffers[MappedBuffer.getBufferIndex(index)];
	}

	@Override
	public void flush() {
		for (ReferenceBuffer buffer : buffers) {
			buffer.flush();
		}
	}

	@Override
	public void drop() {
		int index = 0;
		File file;
		while ((file = getStoreFile(index)).exists()) {
			ReferenceBuffer buffer = buffers[index];
			MappedStoreUtil.deleteBufferAndData(file, buffer.getAtomicBuffer());
			index++;
		}
	}


}


