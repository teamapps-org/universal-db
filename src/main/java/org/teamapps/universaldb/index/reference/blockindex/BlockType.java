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

import java.util.HashMap;
import java.util.Map;

public enum BlockType {

	SINGLE_ENTRY(1, 1, 0),
	ENTRIES_2(2, 2, 0),
	ENTRIES_4(3, 4, 1),
	ENTRIES_8(4, 8, 1),
	ENTRIES_16(5, 16, 1),
	ENTRIES_32(6, 32, 1),
	ENTRIES_64(7, 64, 1),
	ENTRIES_128(8, 128, 2),
	ENTRIES_256(9, 256, 2),
	ENTRIES_512(10, 512, 2),
	ENTRIES_1024(11, 1024, 2),
	ENTRIES_2048(12, 2048, 2),
	ENTRIES_4096_START(13, 4096, 2, 20), //+4B total entries, +8 next block, +8B lastBlock = +20
	ENTRIES_4099_CONTINUE(14, 4099, 2, 8), //+8B next block

	;

	private static Map<Integer, BlockType> blockTypeByEntrySize = new HashMap<>();

	static {
		for (int i = 1; i <= ENTRIES_2048.getMaxEntries(); i++) {
			blockTypeByEntrySize.put(i, calculateEntryType(i));
		}
	}

	private int id;
	private int maxEntries;
	private int entryCountSize;
	private int blockSize;
	private int dataOffset;
	private boolean canContainEmptyEntries;
	private boolean singleBlock;

	private static BlockType calculateEntryType(int entries) {
		BlockType result = null;
		for (BlockType type : BlockType.values()) {
			if (type.getMaxEntries() >= entries) {
				result = type;
				break;
			}
		}
		if (result != null && result != ENTRIES_4099_CONTINUE) {
			return result;
		}
		return ENTRIES_4096_START;
	}

	public static BlockType getEntryType(int entries) {
		if (entries > ENTRIES_2048.getMaxEntries()) {
			return ENTRIES_4096_START;
		} else {
			return blockTypeByEntrySize.get(entries);
		}
	}

	BlockType(int id, int maxEntries, int entryCountSize) {
		this(id, maxEntries, entryCountSize, 0);
	}

	BlockType(int id, int maxEntries, int entryCountSize, int headerDataSize) {
		this.id = id;
		this.maxEntries = maxEntries;
		this.entryCountSize = entryCountSize;
		this.blockSize = 1 + maxEntries * 4 + entryCountSize + headerDataSize;
		dataOffset = 1 + entryCountSize + headerDataSize;
		if (entryCountSize > 0) {
			canContainEmptyEntries = true;
		}
		singleBlock = id < 13;
	}

	public static BlockType getById(int id) {
		return BlockType.values()[id - 1];
	}

	public int getId() {
		return id;
	}

	public int getMaxEntries() {
		return maxEntries;
	}

	public int getEntryCountSize() {
		return entryCountSize;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public int getDataOffset() {
		return dataOffset;
	}

	public boolean canContainEmptyEntries() {
		return canContainEmptyEntries;
	}

	public boolean isSingleBlock() {
		return singleBlock;
	}

	public BlockType getNextSize() {
		if (id < 14) {
			return getById(id + 1);
		} else {
			return getById(14);
		}
	}

	public BlockType getNextSize(int newEntries) {
		int entries = maxEntries + newEntries;
		return getEntryType(entries);
	}


}
