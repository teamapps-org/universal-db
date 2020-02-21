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
package org.teamapps.universaldb.index;

import org.agrona.concurrent.AtomicBuffer;
import org.teamapps.universaldb.util.MappedStoreUtil;

import java.io.File;
import java.nio.MappedByteBuffer;

public abstract class AbstractBufferIndex<TYPE, FILTER> extends AbstractIndex<TYPE, FILTER> {

	private static final int ENTRIES_PER_BUCKET = 100_000;

	private AtomicBuffer[] buffers;
	private int[] bufferEntryOffset;
	private int maximumId;
	private int[] bucketToIndex;

	public AbstractBufferIndex(String name, TableIndex table, FullTextIndexingOptions fullTextIndexingOptions) {
		super(name, table, fullTextIndexingOptions);
		this.maximumId = -1;
		init();
	}

	private void init() {
		int index = 0;
		do {
			addBuffer();
			index++;
		} while (getStoreFile(index).exists());
	}

	protected abstract int getEntrySize();

	private void addBuffer() {
		int index = buffers == null ? 0 : buffers.length;
		int entries = getEntryCountForIndex(index);
		int bufferSize = entries * getEntrySize();
		int buckets = entries / ENTRIES_PER_BUCKET;

		AtomicBuffer buffer = MappedStoreUtil.createAtomicBuffer(getStoreFile(index), bufferSize);
		if (index == 0) {
			buffers = new AtomicBuffer[1];
			buffers[0] = buffer;
			bufferEntryOffset = new int[1];
			bufferEntryOffset[0] = entries;
		} else {
			AtomicBuffer[] newBuffers = new AtomicBuffer[index + 1];
			System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
			newBuffers[buffers.length] = buffer;
			buffers = newBuffers;
			int[] newBufferEntryOffset = new int[index + 1];
			System.arraycopy(bufferEntryOffset, 0, newBufferEntryOffset, 0, bufferEntryOffset.length);
			newBufferEntryOffset[bufferEntryOffset.length] = bufferEntryOffset[bufferEntryOffset.length - 1] + entries;
			bufferEntryOffset = newBufferEntryOffset;
		}
		int startSize = bucketToIndex == null ? 0 : bucketToIndex.length;
		int newSize = startSize + buckets;
		int[] newBucketToIndex = new int[newSize];
		if (startSize > 0) {
			System.arraycopy(bucketToIndex, 0, newBucketToIndex, 0, startSize);
		}
		for (int i = startSize; i < newSize; i++) {
			newBucketToIndex[i] = index;
		}
		bucketToIndex = newBucketToIndex;
		maximumId += entries;
	}

	protected void ensureBufferSize(int id) {
		if (id > maximumId) {
			while (maximumId < id) {
				addBuffer();
			}
		}
	}

	private File getStoreFile(int index) {
		return new File(getPath(), getName() + "-" + index + ".idx");
	}

	private int getEntryCountForIndex(int index) {
		index = Math.min(10, index);
		return (int) Math.pow(2, index) * ENTRIES_PER_BUCKET;
	}

	protected int getIndexForId(int id) {
		int bucket = id / ENTRIES_PER_BUCKET;
		return bucketToIndex[bucket];
	}

	protected int getOffsetForIndex(int index) {
		int offset = 0;
		if (index > 0) {
			offset = bufferEntryOffset[index - 1];
		}
		return offset;
	}

	protected AtomicBuffer getBuffer(int index) {
		return buffers[index];
	}

	public int getMaximumId() {
		return maximumId;
	}

	@Override
	public void close() {
		for (AtomicBuffer buffer : buffers) {
			MappedByteBuffer byteBuffer = (MappedByteBuffer) buffer.byteBuffer();
			byteBuffer.force();
		}
	}

	@Override
	public void drop() {
		int bufferIndex = 0;
		while(getStoreFile(bufferIndex).exists()) {
			try {
				File file = getStoreFile(bufferIndex);
				AtomicBuffer buffer = buffers[bufferIndex];
				MappedStoreUtil.deleteBufferAndData(file, buffer);
			} catch (Throwable e) {
				e.printStackTrace();
			}
			bufferIndex++;
		}
	}
}
