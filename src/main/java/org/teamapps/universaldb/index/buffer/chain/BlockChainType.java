/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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

import java.util.HashMap;
import java.util.Map;

public enum BlockChainType {

	ENTRIES_4(4),
	ENTRIES_8(8),
	ENTRIES_16(16),
	ENTRIES_32(32),
	ENTRIES_64(64),
	ENTRIES_128(128),
	ENTRIES_256(256),
	ENTRIES_512(512),
	ENTRIES_1024(1024),
	ENTRIES_2048(2048),
	ENTRIES_4096_CHAIN(4096),
	;

	private static Map<Integer, BlockChainType> blockTypeByLength = new HashMap<>();
	static {
		for (BlockChainType value : values()) {
			blockTypeByLength.put(value.getBlockLength(), value);
		}

	}

	private final int items;
	private final int length;

	BlockChainType(int items) {
		this.items = items;
		this.length = (items * 4) + getDataOffset() - 4;
	}

	public int getItems() {
		return items;
	}

	public int getBlockLength() {
		return length;
	}

	public int getDataOffset() {
		return isChain() ? 28 : 8;
	}

	public boolean isChain() {
		return items == 4096;
	}
	
	public BlockChainType getNext() {
		switch (this) {
			case ENTRIES_4: return ENTRIES_8;
			case ENTRIES_8: return ENTRIES_16;
			case ENTRIES_16: return ENTRIES_32;
			case ENTRIES_32: return ENTRIES_64;
			case ENTRIES_64: return ENTRIES_128;
			case ENTRIES_128: return ENTRIES_256;
			case ENTRIES_256: return ENTRIES_512;
			case ENTRIES_512: return ENTRIES_1024;
			case ENTRIES_1024: return ENTRIES_2048;
			case ENTRIES_2048: return ENTRIES_4096_CHAIN;
		}
		return null;
	}

	public static BlockChainType getTypeBySize(int size) {
		if (size <= 4) {
			return ENTRIES_4;
		} else if (size <= 8) {
			return ENTRIES_8;
		} else if (size <= 16) {
			return ENTRIES_16;
		} else if (size <= 32) {
			return ENTRIES_32;
		} else if (size <= 64) {
			return ENTRIES_64;
		} else if (size <= 128) {
			return ENTRIES_128;
		} else if (size <= 256) {
			return ENTRIES_256;
		} else if (size <= 512) {
			return ENTRIES_512;
		} else if (size <= 1024) {
			return ENTRIES_1024;
		} else if (size <= 2048) {
			return ENTRIES_2048;
		}
		return ENTRIES_4096_CHAIN;
	}

	public static BlockChainType getTypeByLength(int length) {
		return blockTypeByLength.get(length);
	}
}
