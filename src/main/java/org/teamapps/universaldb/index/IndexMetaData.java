package org.teamapps.universaldb.index;

import org.teamapps.universaldb.index.log.RandomAccessStore;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;

public class IndexMetaData {
	private final static int INDEX_TYPE_POS = 0;
	private final static int CREATION_TIME_POS = 4;
	private final static int MAPPING_ID_POS = 12;
	private final static int ENCRYPTION_POS = 16;
	private final static int NONCE_POS = 20;
	private final static int COUNTER_OFFSET_POS = 36;
	private final static int FQN_POS = 100;

	private final RandomAccessStore randomAccessStore;

	public IndexMetaData(File dataPath, String name, String fqn, int indexType) {
		this.randomAccessStore = new RandomAccessStore(dataPath, name + ".mdx");
		if (randomAccessStore.getSize() == 0) {
			try {
				randomAccessStore.writeInt(INDEX_TYPE_POS, indexType);
				randomAccessStore.writeLong(CREATION_TIME_POS, System.currentTimeMillis());
				SecureRandom secureRandom = new SecureRandom();
				byte[] nonce = new byte[16];
				secureRandom.nextBytes(nonce);
				int ctrOffset = Math.abs(secureRandom.nextInt());
				if (ctrOffset > 1000_000) ctrOffset /= 1000;
				randomAccessStore.write(NONCE_POS, nonce);
				randomAccessStore.writeInt(COUNTER_OFFSET_POS, ctrOffset);
				randomAccessStore.writeString(FQN_POS, fqn);
			} catch (IOException e) {
				throw new RuntimeException("Error creating index meta data", e);
			}
		}
	}

	public int getMappingId() {
		try {
			return randomAccessStore.readInt(MAPPING_ID_POS);
		} catch (IOException e) {
			throw new RuntimeException("Error writing reading meta data mapping id", e);
		}
	}

	public void setMappingId(int mappingId) {
		try {
			randomAccessStore.writeInt(MAPPING_ID_POS, mappingId);
		} catch (IOException e) {
			throw new RuntimeException("Error writing index meta data mapping id", e);
		}
	}

	public int getIndexType() throws IOException {
		return randomAccessStore.readInt(INDEX_TYPE_POS);
	}

	public long getCreationTime() throws IOException {
		return randomAccessStore.readLong(CREATION_TIME_POS);
	}

	public byte[] getNonce()  {
		try {
			return randomAccessStore.read(NONCE_POS, 16);
		} catch (IOException e) {
			throw new RuntimeException("Error reading index meta data", e);
		}
	}

	public int getCtrOffset() {
		try {
			return randomAccessStore.readInt(COUNTER_OFFSET_POS);
		} catch (IOException e) {
			throw new RuntimeException("Error reading index meta data", e);
		}
	}

	public String getFqn() throws IOException {
		return randomAccessStore.readString(FQN_POS);
	}

	public void setEncrypted(boolean encrypted) throws IOException {
		randomAccessStore.writeBoolean(ENCRYPTION_POS, encrypted);
	}

	public boolean isEncrypted() throws IOException {
		return randomAccessStore.readBoolean(ENCRYPTION_POS);
	}
}
