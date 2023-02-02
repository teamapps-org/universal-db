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

import java.io.File;

public class PrimitiveEntryAtomicStore extends AbstractResizingAtomicStore {

	protected static final int BYTE_ENTRIES_PER_FILE = MAX_FILE_SIZE;
	protected static final int SHORT_ENTRIES_PER_FILE = MAX_FILE_SIZE / 2;
	protected static final int INTEGER_ENTRIES_PER_FILE = MAX_FILE_SIZE / 4;
	protected static final int LONG_ENTRIES_PER_FILE = MAX_FILE_SIZE / 8;
	private static final byte[] BIT_MASKS = new byte[8];

	static {
		BIT_MASKS[0] = 0b00000001;
		BIT_MASKS[1] = 0b00000010;
		BIT_MASKS[2] = 0b00000100;
		BIT_MASKS[3] = 0b00001000;
		BIT_MASKS[4] = 0b00010000;
		BIT_MASKS[5] = 0b00100000;
		BIT_MASKS[6] = 0b01000000;
		BIT_MASKS[7] = (byte) 0b10000000;
	}

	public PrimitiveEntryAtomicStore(File path, String name) {
		super(path, name);
	}

	public boolean getBoolean(int id) {
		if (id > getTotalCapacity() * 8 - 1) {
			return false;
		}
		AtomicBuffer buffer = getBuffer(0);
		byte b = buffer.getByte(id / 8);
		int bit = id % 8;
		return (b & BIT_MASKS[bit]) == BIT_MASKS[bit];
	}

	public void setBoolean(int id, boolean value) {
		ensureCapacity(id / 8 + 1);
		AtomicBuffer buffer = getBuffer(0);
		int pos = id / 8;
		byte b = buffer.getByte(pos);
		int bit = id % 8;
		if (value) {
			b = (byte) (b | BIT_MASKS[bit]);
		} else {
			b = (byte) (b & ~BIT_MASKS[bit]);
		}
		buffer.putByte(pos, b);
	}

	public byte getByte(int id) {
		if (id <= 0 || id > getMaximumId(1)) {
			return 0;
		}
		int bufferIndex = id / BYTE_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, BYTE_ENTRIES_PER_FILE, 1);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		return buffer.getByte(offset);
	}

	public void setByte(int id, byte value) {
		ensureCapacity(id, 1);
		int bufferIndex = id / BYTE_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, BYTE_ENTRIES_PER_FILE, 1);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		buffer.putByte(offset, value);
	}

	public short getShort(int id) {
		if (id <= 0 || id > getMaximumId(2)) {
			return 0;
		}
		int bufferIndex = id / SHORT_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, SHORT_ENTRIES_PER_FILE, 2);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		return buffer.getShort(offset, byteOrder);
	}

	public void setShort(int id, short value) {
		ensureCapacity(id, 2);
		int bufferIndex = id / SHORT_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, SHORT_ENTRIES_PER_FILE, 2);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		buffer.putShort(offset, value, byteOrder);
	}


	public int getInt(int id) {
		if (id <= 0 || id > getMaximumId(4)) {
			return 0;
		}
		int bufferIndex = id / INTEGER_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, INTEGER_ENTRIES_PER_FILE, 4);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		return buffer.getInt(offset, byteOrder);
	}

	public void setInt(int id, int value) {
		ensureCapacity(id, 4);
		int bufferIndex = id / INTEGER_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, INTEGER_ENTRIES_PER_FILE, 4);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		buffer.putInt(offset, value, byteOrder);
	}

	public float getFloat(int id) {
		if (id <= 0 || id > getMaximumId(4)) {
			return 0;
		}
		int bufferIndex = id / INTEGER_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, INTEGER_ENTRIES_PER_FILE, 4);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		return buffer.getFloat(offset, byteOrder);
	}

	public void setFloat(int id, float value) {
		ensureCapacity(id, 4);
		int bufferIndex = id / INTEGER_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, INTEGER_ENTRIES_PER_FILE, 4);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		buffer.putFloat(offset, value, byteOrder);
	}

	public long getLong(int id) {
		if (id <= 0 || id > getMaximumId(8)) {
			return 0;
		}
		int bufferIndex = id / LONG_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, LONG_ENTRIES_PER_FILE, 8);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		return buffer.getLong(offset, byteOrder);
	}

	public void setLong(int id, long value) {
		ensureCapacity(id, 8);
		int bufferIndex = id / LONG_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, LONG_ENTRIES_PER_FILE, 8);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		buffer.putLong(offset, value, byteOrder);
	}

	public double getDouble(int id) {
		if (id <= 0 || id > getMaximumId(8)) {
			return 0;
		}
		int bufferIndex = id / LONG_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, LONG_ENTRIES_PER_FILE, 8);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		return buffer.getDouble(offset, byteOrder);
	}

	public void setDouble(int id, double value) {
		ensureCapacity(id, 8);
		int bufferIndex = id / LONG_ENTRIES_PER_FILE;
		int offset = getOffset(id, bufferIndex, LONG_ENTRIES_PER_FILE, 8);
		AtomicBuffer buffer = getBuffer(bufferIndex);
		buffer.putDouble(offset, value, byteOrder);
	}

	protected AtomicBuffer getFirstBuffer() {
		return getBuffer(0);
	}

}
