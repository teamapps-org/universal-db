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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

public class AbstractBlockEntryAtomicStore extends AbstractResizingAtomicStore {

	private final PrimitiveEntryAtomicStore positionBuffer;
	private long freeSpacePosition;
	private int maxDeletionLengthEntries = 1_000;
	private int maxDeletionListSize = 100_000;
	private Map<Integer, Deque<Long>> deletedEntriesMap = createDeletionEntriesMap();

	public AbstractBlockEntryAtomicStore(File path, String name) {
		super(path, name);
		positionBuffer = new PrimitiveEntryAtomicStore(path, name + "-pos");
		init();
		findAllDeletedBlocks();
	}

	private LinkedHashMap<Integer, Deque<Long>> createDeletionEntriesMap() {
		return new LinkedHashMap<>(16, 0.75f, true) {
			@Override
			protected boolean removeEldestEntry(Map.Entry<Integer, Deque<Long>> eldest) {
				return size() > maxDeletionLengthEntries;
			}
		};
	}

	private void init() {
		freeSpacePosition = positionBuffer.getFirstBuffer().getLong(0, byteOrder);
		if (freeSpacePosition == 0) {
			freeSpacePosition = 8;
		}
	}

	private void findAllDeletedBlocks() {
		Map<Integer, Deque<Long>> deletedEntriesMap = createDeletionEntriesMap();
		AtomicBuffer[] buffers = getBuffers();
		for (int i = 0; i < buffers.length; i++) {
			AtomicBuffer buffer = buffers[i];
			int offset = i == 0 ? 8 : 0;
			int capacity = buffer.capacity();
			while (offset + 4 < capacity) {
				int value = buffer.getInt(offset);
				if (value < 0) {
					long deletedPosition = ((long) i * MAX_FILE_SIZE) + offset;
					Deque<Long> positions = deletedEntriesMap.computeIfAbsent(Math.abs(value), len -> new ArrayDeque<>());
					if (positions.size() < maxDeletionListSize) {
						positions.add(deletedPosition);
					}
				}
				if (value == 0) {
					break;
				}
				offset += 4 + Math.abs(value);
			}
		}
		this.deletedEntriesMap = deletedEntriesMap;
	}

	protected Long getFreeSlot(int length) {
		Deque<Long> positions = deletedEntriesMap.get(length);
		return positions != null ? positions.pollFirst() : null;
	}

	protected void setFreeSpacePosition(long position) {
		freeSpacePosition = position;
		positionBuffer.setLong(0, position);
	}

	public long getFreeSpacePosition() {
		return freeSpacePosition;
	}

	public int getBlockLength(int id) {
		long position = positionBuffer.getLong(id);
		if (position > 0) {
			int bufferIndex = getBufferIndex(position);
			int offset = getOffset(position, bufferIndex);
			AtomicBuffer atomicBuffer = getBuffer(bufferIndex);
			return atomicBuffer.getInt(offset);
		}
		return 0;
	}

	public boolean isEmpty(int id) {
		return positionBuffer.getLong(id) == 0;
	}

	protected void removeEntry(long position) {
		if (position > 0) {
			int bufferIndex = getBufferIndex(position);
			int offset = getOffset(position, bufferIndex);
			AtomicBuffer atomicBuffer = getBuffer(bufferIndex);
			int length = atomicBuffer.getInt(offset);
			atomicBuffer.putInt(offset, -1 * length, byteOrder);
			Deque<Long> positions = deletedEntriesMap.computeIfAbsent(length, len -> new ArrayDeque<>());
			if (positions.size() < maxDeletionListSize) {
				positions.add(position);
			}
		}
	}

	protected long getBlockPosition(int id) {
		return positionBuffer.getLong(id);
	}

	protected void setBlockPosition(int id, long position) {
		positionBuffer.setLong(id, position);
	}

	public void drop() {
		positionBuffer.drop();
		super.drop();
	}
}
