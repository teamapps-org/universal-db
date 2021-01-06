/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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

import java.io.File;

public class ReferenceBuffer extends MappedBuffer {

	public ReferenceBuffer(File file, int bufferIndex) {
		super(file, bufferIndex);
	}

	public ReferenceBlock getBlock(long index) {
		int blockPosition = getBlockPosition(index);
		return new ReferenceBlock(index, this, blockPosition, getBlockType(blockPosition));
	}

	public void setBlockData(long index, ReferenceBlock block) {
		int blockPosition = getBlockPosition(index);
		block.reset(index, this, blockPosition, getBlockType(blockPosition));
	}

	public void addBlock(ReferenceBlock writerBlock, BlockType blockType) {
		int blockPosition = getFreeSpacePosition();
		setFreeSpacePosition(blockPosition + blockType.getBlockSize());
		long index = createIndex(blockPosition);
		writerBlock.reset(index, this, blockPosition, blockType);
		writeByte(blockType.getId(), blockPosition);
	}


	private BlockType getBlockType(int blockPosition) {
		return BlockType.getById(Math.abs(readByte(blockPosition)));
	}


	private int getRemainingEntries(int blockPosition) {
		BlockType blockType = BlockType.getById(readByte(blockPosition));
		if (!blockType.canContainEmptyEntries()) {
			return 0;
		}
		int entries = readValue(blockPosition + 1, blockType.getEntryCountSize());
		return blockType.getMaxEntries() - entries;
	}

}
