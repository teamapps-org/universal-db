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
package org.teamapps.universaldb.index.text;

import org.apache.commons.io.IOUtils;
import org.teamapps.universaldb.util.MappedStoreUtil;
import org.teamapps.universaldb.index.reference.blockindex.MappedBuffer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class CharIndex {

	private final File path;
	private final String name;
	private MappedBuffer[] buffers;
	private MappedBuffer currentBuffer;

	private Map<Integer, Deque<Long>> deletedBlocksByLength;

	public CharIndex(File path, String name) {
		this.path = path;
		this.name = name;
		deletedBlocksByLength = new HashMap<>();
		init();
	}

	private void init() {
		int index = 0;
		do {
			addBuffer(index);
			index++;
		} while (getStoreFile(index).exists());
	}

	private void addBuffer() {
		addBuffer(buffers.length);
	}

	private void addBuffer(int index) {
		MappedBuffer buffer = new MappedBuffer(getStoreFile(index), index);
		currentBuffer = buffer;
		if (index == 0) {
			buffers = new MappedBuffer[1];
			buffers[0] = buffer;
		} else {
			MappedBuffer[] newBuffers = new MappedBuffer[index + 1];
			System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
			newBuffers[buffers.length] = buffer;
			buffers = newBuffers;
		}
	}

	private File getStoreFile(int index) {
		return new File(path, name + "-" + index + ".cdx");
	}

	private MappedBuffer getBufferForIndex(long index) {
		return buffers[MappedBuffer.getBufferIndex(index)];
	}

	public String getText(long index) {
		MappedBuffer buffer = getBufferForIndex(index);
		int position = MappedBuffer.getBlockPosition(index);
		int len = buffer.readInt(position);
		if (len < 0) {
			return null;
		}
		byte[] bytes = new byte[len];
		buffer.readBytes(position + 4, bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	public int getTextByteLength(long index) {
		MappedBuffer buffer = getBufferForIndex(index);
		int position = MappedBuffer.getBlockPosition(index);
		return buffer.readInt(position);
	}

	public long setText(String text) {
		if (text == null || text.isEmpty()) {
			return 0;
		}
		byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
		while (currentBuffer.getRemainingSize() < bytes.length + 4) {
			addBuffer();
		}
		int position = currentBuffer.getFreeSpacePosition();
		long index = currentBuffer.createIndex(position);

		currentBuffer.writeInt(bytes.length, position);
		currentBuffer.writeValue(bytes, position + 4);
		currentBuffer.setFreeSpacePosition(position + 4 + bytes.length);
		return index;
	}

	public void removeText(long index) {
		MappedBuffer buffer = getBufferForIndex(index);
		int position = MappedBuffer.getBlockPosition(index);
		int len = buffer.readInt(position);
		len = Math.abs(len) * -1;

	}

	public void close() {
		for (MappedBuffer buffer : buffers) {
			buffer.flush();
		}
	}

	public void drop() {
		close();
		int bufferIndex = 0;
		while(getStoreFile(bufferIndex).exists()) {
			try {
				File file = getStoreFile(bufferIndex);
				MappedBuffer buffer = buffers[bufferIndex];
				MappedStoreUtil.deleteBufferAndData(file, buffer.getAtomicBuffer());
			} catch (Throwable e) {
				e.printStackTrace();
			}
			bufferIndex++;
		}
	}

}
