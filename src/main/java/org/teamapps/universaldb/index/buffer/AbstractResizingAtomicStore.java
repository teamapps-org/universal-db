package org.teamapps.universaldb.index.buffer;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class AbstractResizingAtomicStore {
	final static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	protected static final ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
	protected static final int MIN_FILE_SIZE = 120_000;
	protected static final int MAX_FILE_SIZE = 1966_080_000;


	private final File path;
	private final String name;
	private final long maxTotalSize;
	private AtomicBuffer[] buffers;
	private int lastBufferSize;
	private long totalCapacity;


	public AbstractResizingAtomicStore(File path, String name) {
		this.path = path;
		this.name = name;
		this.maxTotalSize = MAX_FILE_SIZE * 8L;
		init();
	}

	private void init() {
		int index = 0;
		buffers = new AtomicBuffer[1];
		if (!getStoreFile(0).exists()) {
			updateBufferSize(0, MIN_FILE_SIZE);
		} else {
			while (index == 0 || getStoreFile(index).exists()) {
				File storeFile = getStoreFile(index);
				updateBufferSize(index, (int) storeFile.length());
				index++;
			}
		}
	}

	protected void ensureCapacity(int id, int byteLength) {
		ensureCapacity((id + 1L) * byteLength);
	}

	protected long findNextBlockPosition(long position, int blockSize) {
		int bufferIndex = getBufferIndex(position);
		int offset = getOffset(position, bufferIndex);
		if (blockSize > (lastBufferSize - offset) && lastBufferSize == MAX_FILE_SIZE) {
			return (long) buffers.length * MAX_FILE_SIZE + blockSize;
		}
		return position;
	}

	protected AtomicBuffer getBuffer(int index) {
		return buffers[index];
	}

	protected AtomicBuffer[] getBuffers() {
		return buffers;
	}

	protected int getBufferIndex(long position) {
		return (int) (position / MAX_FILE_SIZE);
	}

	protected int getOffset(long position, int bufferIndex) {
		return (int) (position - ((long) bufferIndex * MAX_FILE_SIZE));
	}

	protected int getOffset(int id, int bufferIndex, int entriesPerFile, int byteLength) {
		return (id - (bufferIndex * entriesPerFile)) * byteLength;
	}

	protected int getMaximumId(int byteLength) {
		return (int) (totalCapacity / byteLength) - 1;
	}

	protected void ensureCapacity(long size) {
		if (size > totalCapacity) {
			if (size > maxTotalSize) {
				throw new RuntimeException("Index size exceeding maximum, requested size: " + size + ", index:" + toString());
			}
			while (size > totalCapacity) {
				if (lastBufferSize < MAX_FILE_SIZE) {
					updateBufferSize(buffers.length - 1, lastBufferSize * 2);
				} else {
					updateBufferSize(buffers.length, MAX_FILE_SIZE / 4);
				}
			}
		}
	}

	private void updateBufferSize(int bufferIndex, int bufferSize) {
		System.out.println("update size, index:" + bufferIndex + ", size:" + bufferSize);
		File file = getStoreFile(bufferIndex);
		try {
			RandomAccessFile ras = new RandomAccessFile(file, "rw");
			if (!file.exists() || file.length() < bufferSize) {
				ras.seek(bufferSize - 4);
				ras.write(new byte[4]);
			}
			MappedByteBuffer mappedByteBuffer = ras.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
			try {
				ras.close();
			} catch (Throwable t) {
				logger.warn("Error releasing RAS file on buffer creation:" + t.getMessage() + ", file:" + file);
			}
			if (buffers.length >= bufferIndex) {
				AtomicBuffer[] newBuffers = new AtomicBuffer[bufferIndex + 1];
				System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
				buffers = newBuffers;
			}
			AtomicBuffer buffer = new UnsafeBuffer(mappedByteBuffer);
			buffers[bufferIndex] = buffer;
			lastBufferSize = bufferSize;
			totalCapacity = ((long) MAX_FILE_SIZE * bufferIndex) + bufferSize;
		} catch (IOException e) {
			throw new RuntimeException("ERROR: updating buffer size of buffer:" + file.getPath(), e);
		}
	}

	public File getPath() {
		return path;
	}

	public String getName() {
		return name;
	}

	public long getTotalCapacity() {
		return totalCapacity;
	}

	private File getStoreFile(int index) {
		return new File(getPath(), getName() + "-" + index + ".idx");
	}

	public void drop() {
		try {
			buffers = null;
			int index = 0;
			while (index == 0 || getStoreFile(index).exists()) {
				File storeFile = getStoreFile(index);
				storeFile.delete();
				index++;
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "ResizingAtomicMappedBuffer{" +
				"path=" + path +
				", name='" + name + '\'' +
				", totalCapacity=" + totalCapacity +
				'}';
	}
}
