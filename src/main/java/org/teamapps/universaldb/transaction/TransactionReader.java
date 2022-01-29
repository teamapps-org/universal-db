/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2022 TeamApps.org
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
package org.teamapps.universaldb.transaction;

import org.teamapps.universaldb.index.DataBaseMapper;

import java.io.*;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

public class TransactionReader implements Iterator<ClusterTransaction> {

	private final File transactionFile;
	private final DataBaseMapper dataBaseMapper;
	private boolean hasNext;
	private final DataInputStream dataInputStream;
	private int currentFilePos = 8;

	public TransactionReader(File transactionFile, DataBaseMapper dataBaseMapper) throws IOException {
		this.transactionFile = transactionFile;
		if (transactionFile.getName().endsWith("z")) {
			dataInputStream = new DataInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(transactionFile))));
		} else {
			dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(transactionFile)));
		}
		this.dataBaseMapper = dataBaseMapper;
		dataInputStream.readLong();
		int nextPacket = dataInputStream.read();
		hasNext = nextPacket > 0;
	}


	private boolean hasMoreTransactions(int filePos, int size) {
		return 1L + filePos + size < TransactionStore.MAX_TRANSACTION_FILE_SIZE;
	}

	private ClusterTransaction readTransaction() {
		try {
			int size = dataInputStream.readInt();
			byte[] bytes = new byte[size];
			dataInputStream.read(bytes);
			TransactionPacket packet = new TransactionPacket(bytes);
			ClusterTransaction transaction = new ClusterTransaction(packet, dataBaseMapper);
			hasNext = hasMoreTransactions(currentFilePos, size + 5);
			if (hasNext) {
				int nextPacket = dataInputStream.read();
				hasNext = nextPacket > 0;
			}
			return transaction;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public ClusterTransaction next() {
		return readTransaction();
	}

}
