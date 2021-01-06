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
package org.teamapps.universaldb.index.reference.blockindex.iterator;

import org.teamapps.universaldb.index.reference.blockindex.ReferenceBlockProvider;
import org.teamapps.universaldb.index.reference.blockindex.ReferenceBlock;

import java.util.PrimitiveIterator;

public class ChainedBlockIterator implements PrimitiveIterator.OfInt {

	private final ReferenceBlockProvider blockProvider;

	private ReferenceBlock block;
	private ReferenceBlock nextBlock;
	private int nextValue;
	private int dataOffset;
	private int blockEntryPosition;
	private int blockEntries;


	public ChainedBlockIterator(ReferenceBlock block, ReferenceBlockProvider blockProvider) {
		this.blockProvider = blockProvider;
		this.block = block;
		setBlockData(block);
		retrieveNextValue();
	}

	private void setBlockData(ReferenceBlock block) {
		dataOffset = block.getBlockDataPosition();
		blockEntryPosition = 0;
		blockEntries = block.getBlockEntryCount();
		nextBlock = blockProvider.getBlock(block.getNextIndex());
	}

	private void retrieveNextValue() {
		if (nextBlock == null && blockEntryPosition >= blockEntries) {
			nextValue = 0;
			return;
		}
		nextValue = block.getBuffer().readInt(dataOffset + blockEntryPosition * 4);
		blockEntryPosition++;
		if (blockEntryPosition == blockEntries && nextBlock != null) {
			block = nextBlock;
			setBlockData(nextBlock);
		}
		if (nextValue < 0) {
			retrieveNextValue();
		}
	}

	@Override
	public int nextInt() {
		int value = nextValue;
		retrieveNextValue();
		return value;
	}

	@Override
	public boolean hasNext() {
		return nextValue > 0;


	}
}
