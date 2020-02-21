/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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

public class ReferenceBlock {

	private long index;
	private ReferenceBuffer buffer;
	private int blockPosition;
	private BlockType blockType;

	public ReferenceBlock(long index, ReferenceBuffer buffer, int blockPosition, BlockType blockType) {
		this.index = index;
		this.buffer = buffer;
		this.blockPosition = blockPosition;
		this.blockType = blockType;
	}

	public ReferenceBlock(ReferenceBlock block) {
		this.index = block.getIndex();
		this.buffer = block.getBuffer();
		this.blockPosition = block.getBlockPosition();
		this.blockType = block.getBlockType();
	}

	public void reset(long index, ReferenceBuffer buffer, int blockPosition, BlockType blockType) {
		this.index = index;
		this.buffer = buffer;
		this.blockPosition = blockPosition;
		this.blockType = blockType;
	}

	public ReferenceBlock copy() {
		return new ReferenceBlock(this);
	}

	public int getTotalEntryCount() {
		if (blockType == BlockType.ENTRIES_4096_START) {
			return buffer.readInt(blockPosition + 3);
		} else if (blockType.getEntryCountSize() == 0) {
			return blockType.getId();
		} else {
			return getBlockEntryCount();
		}
	}

	public int getBlockEntryCount() {
		if (!blockType.canContainEmptyEntries()) {
			return blockType.getId();
		}
		return buffer.readValue(blockPosition + 1, blockType.getEntryCountSize());
	}

	public int getRemainingEntries() {
		return blockType.getMaxEntries() - getBlockEntryCount();

	}

	void writeBlockEntryCount(int count) {
		buffer.writeValue(count, blockPosition + 1, blockType.getEntryCountSize());
	}

	void addValue(int value, int entryOffset) {
		buffer.writeInt(value, blockPosition + blockType.getDataOffset() + entryOffset);
	}


	boolean addValueIfRemainingSpace(int value) {
		if (!blockType.canContainEmptyEntries()) {
			return false;
		}
		int entries = buffer.readValue(blockPosition + 1, blockType.getEntryCountSize());
		if (blockType.getMaxEntries() - entries == 0) {
			return false;
		}
		buffer.writeInt(value, blockPosition + blockType.getDataOffset() + entries * 4);
		buffer.writeValue(entries + 1, blockPosition + 1, blockType.getEntryCountSize());
		return true;
	}

	private byte[] readFullBlockContent() {
		byte[] bytes = new byte[blockType.getMaxEntries() * 4];
		buffer.readBytes(blockPosition + blockType.getDataOffset(), bytes);
		return bytes;
	}

	private byte[] readBlockContent(int entries) {
		byte[] bytes = new byte[entries * 4];
		buffer.readBytes(blockPosition + blockType.getDataOffset(), bytes);
		return bytes;
	}

	void copyContent(ReferenceBlock sourceBlock) {
		buffer.writeValue(sourceBlock.readFullBlockContent(), blockPosition + blockType.getDataOffset());
	}

	void copyContent(ReferenceBlock sourceBlock, int entries) {
		buffer.writeValue(sourceBlock.readBlockContent(entries), blockPosition + blockType.getDataOffset());
	}

	void writeStartBlockTotalCount(int count) {
		buffer.writeInt(count, blockPosition + 3);
	}

	void writeStartBlockNextIndex(long nextIndex) {
		buffer.writeLong(nextIndex, blockPosition + 7);
	}

	void writeStartBlockLastIndex(long lastIndex) {
		buffer.writeLong(lastIndex, blockPosition + 15);
	}

	void writeNextIndex(long nextIndex) {
		if (blockType == BlockType.ENTRIES_4096_START) {
			writeStartBlockNextIndex(nextIndex);
		} else {
			buffer.writeLong(nextIndex, blockPosition + 3);
		}
	}

	public long getNextIndex() {
		if (blockType == BlockType.ENTRIES_4096_START) {
			return getStartBlockNextIndex();
		} else {
			return buffer.readLong(blockPosition + 3);
		}
	}

	public int getStartBlockTotalCount() {
		return buffer.readInt(blockPosition + 3);
	}

	public long getStartBlockNextIndex() {
		return buffer.readLong(blockPosition + 7);
	}

	public long getStartBlockLastIndex() {
		return buffer.readLong(blockPosition + 15);
	}

	public long getIndex() {
		return index;
	}

	public ReferenceBuffer getBuffer() {
		return buffer;
	}

	public void setBuffer(ReferenceBuffer buffer) {
		this.buffer = buffer;
	}

	public int getBlockPosition() {
		return blockPosition;
	}

	public int getBlockDataPosition() {
		return blockPosition + blockType.getDataOffset();
	}


	public BlockType getBlockType() {
		return blockType;
	}


}
