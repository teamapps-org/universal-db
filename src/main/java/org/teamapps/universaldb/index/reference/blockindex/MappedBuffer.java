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

import org.agrona.concurrent.AtomicBuffer;
import org.teamapps.universaldb.util.MappedStoreUtil;

import java.io.File;
import java.nio.MappedByteBuffer;

public class MappedBuffer {

	private final File file;
	private final int bufferIndex;
	private final int size;
	private final AtomicBuffer atomicBuffer;

	private int freeSpacePosition;

	public File getFile() {
		return file;
	}

	public int getBufferIndex() {
		return bufferIndex;
	}

	public int getSize() {
		return size;
	}

	public static long createIndex(int buffer, int position) {
		return (((long) buffer) << 32) | (position & 0xffffffffL);
	}

	public static int getBufferIndex(long index) {
		return (int) (index >> 32);
	}

	public static int getBlockPosition(long index) {
		return (int) index;
	}

	public MappedBuffer(File file, int bufferIndex) {
		this.file = file;
		this.bufferIndex = bufferIndex;
		this.size = calculateSize(bufferIndex);
		atomicBuffer = MappedStoreUtil.createAtomicBuffer(file, size);
		freeSpacePosition = atomicBuffer.getInt(0);
		if (freeSpacePosition < 4) {
			freeSpacePosition = 4;
		}
	}

	private int calculateSize(int bufferIndex) {
		bufferIndex = Math.min(12, bufferIndex);
		return (int) Math.pow(2, bufferIndex) * 250_000;
	}


	public long createIndex(int position) {
		return createIndex(bufferIndex, position);
	}

	public int getRemainingSize() {
		return size - freeSpacePosition;
	}

	public void setFreeSpacePosition(int position) {
		atomicBuffer.putInt(0, position);
		freeSpacePosition = position;
	}

	public int getFreeSpacePosition() {
		return freeSpacePosition;
	}

	public void writeByte(int value, int position) {
		atomicBuffer.putByteVolatile(position, (byte) value);
	}

	public void writeShort(short value, int position) {
		atomicBuffer.putShortVolatile(position, value);
	}

	public void writeInt(int value, int position) {
		atomicBuffer.putIntVolatile(position, value);
	}

	public void writeLong(long value, int position) {
		atomicBuffer.putLongVolatile(position, value);
	}

	public void writeValue(int value, int position, int length) {
		switch (length) {
			case 1:
				writeByte(value, position);
				break;
			case 2:
				writeShort((short) value, position);
				break;
			case 4:
				writeInt(value, position);
				break;
		}
	}

	public void writeValue(byte[] bytes, int position) {
		atomicBuffer.putBytes(position, bytes);
	}

	public int readByte(int position) {
		return atomicBuffer.getByteVolatile(position);
	}

	public short readShort(int position) {
		return atomicBuffer.getShortVolatile(position);
	}

	public int readInt(int position) {
		return atomicBuffer.getIntVolatile(position);
	}

	public long readLong(int position) {
		return atomicBuffer.getLongVolatile(position);
	}

	public void readBytes(int position, byte[] bytes) {
		atomicBuffer.getBytes(position, bytes);
	}

	public int readValue(int position, int length) {
		switch (length) {
			case 1:
				return readByte(position);
			case 2:
				return readShort(position);
			case 4:
				return readInt(position);
		}
		return 0;
	}

	public void flush() {
		MappedByteBuffer buffer = (MappedByteBuffer) atomicBuffer.byteBuffer();
		buffer.force();
	}

	public AtomicBuffer getAtomicBuffer() {
		return atomicBuffer;
	}
}
