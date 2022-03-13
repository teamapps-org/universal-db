package org.teamapps.universaldb.index.nonmap;


import java.io.*;

public class SequentialStore {

	private final File storeFile;
	private final DataOutputStream dos;
	private long position;

	public SequentialStore(File basePath, String name) throws IOException {
		storeFile = new File(basePath, name);
		position = storeFile.length();
		dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(storeFile, true), 16_000));
		if (position == 0) {
			dos.writeInt(1);
			position = 4;
		}
	}

	public synchronized long write(byte[] bytes) throws IOException {
		dos.writeInt(bytes.length);
		dos.write(bytes);
		long storePos = position;
		position += bytes.length + 4;
		return storePos;
	}

	public synchronized long writeCommitted(byte[] bytes) throws IOException {
		long storePos = write(bytes);
		flush();
		return storePos;
	}

	public synchronized byte[] read(long pos) throws IOException {
		RandomAccessFile ras = new RandomAccessFile(storeFile, "r");
		ras.seek(pos);
		int size = ras.readInt();
		byte[] bytes = new byte[size];
		int read = 0;
		while (read < bytes.length) {
			read += ras.read(bytes, read, size - read);
		}
		ras.close();
		return bytes;
	}

	public long getPosition() {
		return position;
	}

	public void flush() {
		try {
			dos.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			dos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void drop() {
		try {
			close();
			storeFile.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
