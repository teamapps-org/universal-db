/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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

import org.agrona.concurrent.AtomicBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.util.MappedStoreUtil;
import org.teamapps.universaldb.schema.Schema;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;


public class TransactionStore implements TransactionIdProvider {

    private static final int TIMESTAMP_FIRST_SYSTEM_START_POS = 0;
    private static final int TIMESTAMP_SYSTEM_START_POS = 8;
    private static final int TIMESTAMP_SHUTDOWN_POS = 16;
    private static final int LAST_TRANSACTION_ID_POS = 24;
    private static final int CURRENT_TRANSACTION_ID_POS = 32;
    private static final int CURRENT_TRANSACTION_FILE_ID_POS = 40;
    private static final int CURRENT_TRANSACTION_FILE_POSITION_POS = 44;
    private static final int TRANSACTIONS_COUNT_POS = 48;

    public static final long MAX_TRANSACTION_FILE_SIZE = Integer.MAX_VALUE;


    private static final int CLUSTER_NODE_TYPE_POS = 56;
    private static final int CLUSTER_CONFIG_POS = 1000;
    private static final int SCHEMA_POS = 30_000;

    private static final Logger log = LoggerFactory.getLogger(TransactionStore.class);

    private File path;
    private AtomicBuffer buffer;
    private static int storeSize =  200_000;

    private long timestampFirstSystemStart;
    private long timestampSystemStart;
    private long timestampShutdown;

    private long lastTransactionId;
    private long currentTransactionId;
    private int currentTransactionFileId;
    private int currentTransactionFilePosition;
    private long transactionCount;


    private File currentTransactionFile;
    private DataOutputStream currentTransactionOutputStream;
    private Schema schema;


    public TransactionStore(File path) throws IOException {
    	this.path = new File(path, "transaction-log");
        File file = new File(this.path, "database.stat");
        buffer = MappedStoreUtil.createAtomicBuffer(file, storeSize);
        init();
    }

    private void init() throws IOException {
        timestampFirstSystemStart = buffer.getLong(TIMESTAMP_FIRST_SYSTEM_START_POS);
        timestampSystemStart = buffer.getLong(TIMESTAMP_SYSTEM_START_POS);
        timestampShutdown = buffer.getLong(TIMESTAMP_SHUTDOWN_POS);
        lastTransactionId = buffer.getLong(LAST_TRANSACTION_ID_POS);
        currentTransactionId = buffer.getLong(CURRENT_TRANSACTION_ID_POS);
        currentTransactionFileId = buffer.getInt(CURRENT_TRANSACTION_FILE_ID_POS);
        currentTransactionFilePosition = buffer.getInt(CURRENT_TRANSACTION_FILE_POSITION_POS);
        transactionCount = buffer.getLong(TRANSACTIONS_COUNT_POS);

        timestampSystemStart = System.currentTimeMillis();
        buffer.putLong(TIMESTAMP_SYSTEM_START_POS, timestampSystemStart);

        if (timestampFirstSystemStart == 0) {
            buffer.putLong(TIMESTAMP_FIRST_SYSTEM_START_POS, timestampSystemStart);
        }
        currentTransactionFile = getTransactionFileByFileId(currentTransactionFileId, path, false);
        currentTransactionOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(currentTransactionFile, true)));

        if (currentTransactionId == 0) {
            currentTransactionId = 8;
            if (currentTransactionFile.length() == 0) {
                currentTransactionOutputStream.writeLong(System.currentTimeMillis());
            }
            currentTransactionFilePosition = 8;
        }

        log.info("Start UniversalDB with path:" + path.getParentFile().getPath());
        log.info("Start transaction-store, file-id:" + currentTransactionFileId + ", file-pos:" + currentTransactionFilePosition + ", transaction-count:" + transactionCount);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                currentTransactionOutputStream.flush();
                log.info("Shutting down transaction-store, file-id:" + currentTransactionFileId + ", file-pos:" + currentTransactionFilePosition + ", transaction-count:" + transactionCount);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

    public Iterator<byte[]> getTransactions(long startTransaction, long lastTransaction) {
        try {
            return new ByteArrayTransactionReader(startTransaction, lastTransactionId, path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

//    public void writeTransaction(Transaction transaction) throws IOException {
//        TransactionRequest transactionRequest = transaction.createRequest();
//        if (cluster != null) {
//
//        } else {
//            executeTransaction(transactionRequest);
//        }
//    }

    public void synchronizeTransaction(ClusterTransaction transaction) throws IOException {
        TransactionRequest transactionRequest = transaction.createRequest();
        executeTransaction(transactionRequest);
    }

    public Schema loadSchema() throws IOException {
        int len = buffer.getInt(SCHEMA_POS);
        if (len == 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        buffer.getBytes(SCHEMA_POS + 4, bytes);
        return new Schema(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    public void saveSchema(Schema schema) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        schema.writeSchema(new DataOutputStream(bos));
        byte[] bytes = bos.toByteArray();
        buffer.putInt(SCHEMA_POS, bytes.length);
        buffer.putBytes(SCHEMA_POS + 4, bytes);
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public synchronized void executeTransaction(TransactionRequest transactionRequest) throws IOException {
        if (transactionRequest.isExecuted()) {
            long transactionId = transactionRequest.getTransaction().getTransactionId();
            if (currentTransactionId != transactionId) {
                //todo: enforce disconnect!
                throw new RuntimeException("Cannot execute transaction with unexptected transaction id, expected" + currentTransactionId + ", actual:" + transactionId);
            }
            transactionRequest.executeResolvedTransaction();
        } else {
            transactionRequest.executeUnresolvedTransaction(this);
        }
        writeTransaction(transactionRequest);
    }

    private void writeTransaction(TransactionRequest transactionRequest) throws IOException {
        if (!transactionRequest.isExecuted()) {
            throw new RuntimeException("Cannot store transaction that has not been executed!");
        }
        TransactionPacket packet = transactionRequest.getPacket();
        byte[] bytes = packet.writePacketBytes();
        checkTransactionFilePosition(transactionRequest.getTransaction());
        transactionCount++;

        currentTransactionOutputStream.write(1); //todo: packet types!
        currentTransactionOutputStream.writeInt(bytes.length);
        currentTransactionOutputStream.write(bytes);
        currentTransactionOutputStream.flush();

        lastTransactionId = currentTransactionId;
        buffer.putLong(LAST_TRANSACTION_ID_POS, lastTransactionId);

        int size = bytes.length + 5;
        if (newTransactionFileRequired(currentTransactionFilePosition, size)) {
            final File fileToZip = currentTransactionFile;
            final File zipFile = getTransactionFileByFileId(currentTransactionFileId, path, true);
            File previousFile = null;
            if (currentTransactionFileId > 0) {
                previousFile = getTransactionFileByFileId(currentTransactionFileId - 1, path, false);
            }

            currentTransactionFileId++;
            currentTransactionFilePosition = 8;
            currentTransactionOutputStream.close();
            currentTransactionFile = getTransactionFileByFileId(currentTransactionFileId, path, true);
            currentTransactionOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(currentTransactionFile, true)));
            currentTransactionOutputStream.writeLong(System.currentTimeMillis());
            compressTransactionFile(fileToZip, zipFile, previousFile);
        } else {
            currentTransactionFilePosition += size;
            buffer.putInt(CURRENT_TRANSACTION_FILE_POSITION_POS, currentTransactionFilePosition);
        }
        buffer.putInt(CURRENT_TRANSACTION_FILE_ID_POS, currentTransactionFileId);
        buffer.putInt(CURRENT_TRANSACTION_FILE_POSITION_POS, currentTransactionFilePosition);
        buffer.putLong(TRANSACTIONS_COUNT_POS, transactionCount);
        currentTransactionId = createTransactionIndex(currentTransactionFileId, currentTransactionFilePosition);
        buffer.putLong(CURRENT_TRANSACTION_ID_POS, currentTransactionId);
    }

    private void checkTransactionFilePosition(ClusterTransaction transaction) {
        long transactionId = transaction.getTransactionId();
        int fileId = getTransactionFileId(transactionId);
        int filePosition = getTransactionFilePosition(transactionId);
        if (currentTransactionFileId != fileId || currentTransactionFilePosition != filePosition) {
            throw new RuntimeException("Wrong transaction with id: " + transactionId + ", expected file-id:" + currentTransactionFileId + ", actual file-id:" + fileId + ", expected file-position:" + currentTransactionFilePosition + ", actual position:" + filePosition);
        }
    }

    private void compressTransactionFile(File fileToZip, File zipFile, File previousFile) {
        new Thread(() -> {
            byte[] buffer = new byte[8096];
            try (GZIPOutputStream gzipOut = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(zipFile)))) {
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(fileToZip));
                int read;
                while ((read = in.read(buffer)) > 0) {
                    gzipOut.write(buffer, 0, read);
                }
                in.close();
                gzipOut.finish();
                gzipOut.close();
                if (previousFile != null &&
                        previousFile.exists() &&
                        new File(previousFile.getParentFile(), previousFile.getName().substring(0, previousFile.getName().length() - 4) + ".bgz").exists()
                ) {
                    previousFile.delete();
                }
                log.info("Compressed transaction file:" + fileToZip.getName());
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }).start();
    }

    @Override
    public long getNextTransactionId() {
        return currentTransactionId;
    }

    protected static boolean newTransactionFileRequired(int filePosition, int packetSize) {
        return 1L + filePosition + packetSize >= MAX_TRANSACTION_FILE_SIZE;
    }

    protected static long createTransactionIndex(int fileId, int filePosition) {
        return (((long) fileId) << 32) | (filePosition & 0xffffffffL);
    }

    protected static int getTransactionFileId(long index) {
        return (int) (index >> 32);
    }

    protected static int getTransactionFilePosition(long index) {
        return (int) index;
    }

    protected static File getTransactionFileByFileId(int fileId, File path, boolean compressed) {
        if (!compressed) {
            return new File(path, "transactions-" + fileId + ".bin");
        } else {
            return new File(path, "transactions-" + fileId + ".bgz");
        }
    }

    public void close() {
        timestampShutdown = System.currentTimeMillis();
        buffer.putLong(TIMESTAMP_SHUTDOWN_POS, timestampShutdown);
        MappedByteBuffer mappedByteBuffer = (MappedByteBuffer) buffer.byteBuffer();
        mappedByteBuffer.force();
    }

    public void drop() {
		File file = new File(path, "database.stat");
    	MappedStoreUtil.deleteBufferAndData(file, buffer);
	}

    public long getCurrentTransactionId() {
        return currentTransactionId;
    }

    public long getLastTransactionId() {
        return lastTransactionId;
    }

    public Schema getSchema() {
        return schema;
    }

    public long getTimestampFirstSystemStart() {
        return timestampFirstSystemStart;
    }

    public long getTimestampSystemStart() {
        return timestampSystemStart;
    }

    public long getTimestampShutdown() {
        return timestampShutdown;
    }

    public int getCurrentTransactionFileId() {
        return currentTransactionFileId;
    }

    public int getCurrentTransactionFilePosition() {
        return currentTransactionFilePosition;
    }

    public long getTransactionCount() {
        return transactionCount;
    }

}
