package org.teamapps.universaldb.index.buffer;

import org.agrona.concurrent.AtomicBuffer;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class BlockEntryAtomicStore extends AbstractBlockEntryAtomicStore {

	public BlockEntryAtomicStore(File path, String name) {
		super(path, name);
	}

	public void setBytes(int id, byte[] bytes) {
		if (id == 0) {
			return;
		}
		long lastPosition = getBlockPosition(id);
		if (bytes == null || bytes.length == 0) {
			setBlockPosition(id, 0);
			removeEntry(lastPosition);
			return;
		}
		int length = bytes.length;
		Long freeSlot = getFreeSlot(length);
		if (freeSlot != null) {
			long position = freeSlot;
			int bufferIndex = getBufferIndex(position);
			int offset = getOffset(position, bufferIndex);
			AtomicBuffer atomicBuffer = getBuffer(bufferIndex);
			if (atomicBuffer.getInt(offset) != (-1 * length)) {
				throw new RuntimeException("Try to reuse deleted block entry that already exists, id:" + id + ", pos:" + position + ", index:" + this);
			}
			atomicBuffer.putInt(offset, length, byteOrder);
			atomicBuffer.putBytes(offset + 4, bytes);
			setBlockPosition(id, position);
		} else {
			long position = findNextBlockPosition(getFreeSpacePosition(), length + 4);
			ensureCapacity(position + length + 4);
			int bufferIndex = getBufferIndex(position);
			int offset = getOffset(position, bufferIndex);
			AtomicBuffer atomicBuffer = getBuffer(bufferIndex);
			atomicBuffer.putInt(offset, length, byteOrder);
			atomicBuffer.putBytes(offset + 4, bytes);
			setBlockPosition(id, position);
			setFreeSpacePosition(position + length + 4);
		}
		removeEntry(lastPosition);
	}

	public byte[] getBytes(int id) {
		long position = getBlockPosition(id);
		if (position > 0) {
			int bufferIndex = getBufferIndex(position);
			int offset = getOffset(position, bufferIndex);
			AtomicBuffer buffer = getBuffer(bufferIndex);
			int len = buffer.getInt(offset);
			byte[] bytes = new byte[len];
			buffer.getBytes(offset + 4, bytes);
			return bytes;
		}
		return null;
	}

	public void removeBytes(int id) {
		long position = getBlockPosition(id);
		if (position > 0) {
			removeEntry(position);
			setBlockPosition(id, 0);
		}
	}

	public void setText(int id, String text) {
		setBytes(id, text == null || text.isEmpty() ? null : text.getBytes(StandardCharsets.UTF_8));
	}

	public String getText(int id) {
		byte[] bytes = getBytes(id);
		return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
	}

	public void removeText(int id) {
		removeBytes(id);
	}

}
