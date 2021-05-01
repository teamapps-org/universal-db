package org.teamapps.universaldb.index;

import org.teamapps.universaldb.index.numeric.LongIndex;
import org.teamapps.universaldb.transaction.TransactionRecord;
import org.teamapps.universaldb.transaction.TransactionRecordValue;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableTransactionIndex {

	private LongIndex positionIndex;
	private long currentPosition;
	private File indexFile;
	private File transactionFile;
	private BufferedOutputStream outputStream;

	public TableTransactionIndex(File path, String tableName) throws IOException {
		transactionFile = new File(path, tableName + ".traidx");
		outputStream = new BufferedOutputStream(new FileOutputStream(transactionFile, true));

		if (transactionFile.length() == 0) {
			outputStream.write(tableName.getBytes(StandardCharsets.UTF_8));
			outputStream.flush();
		}
		currentPosition = transactionFile.length();
	}

	public void writeTransactionRecord(TransactionRecord transactionRecord) throws IOException {
		int id = transactionRecord.getRecordId();
		if (id <= 0) {
			throw new RuntimeException("Transaction record with wrong ID:" + id + ", path:" + transactionFile.getPath());
		}
		long previousPosition = positionIndex.getValue(id);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		List<TransactionRecordValue> recordValues = transactionRecord.getRecordValues();
		dos.writeLong(previousPosition);
		dos.writeInt(id);
		dos.writeBoolean(transactionRecord.isUpdate());
		dos.writeBoolean(transactionRecord.isDeleteRecord());
		dos.writeLong(transactionRecord.getRecordTransactionId());
		dos.writeInt(recordValues.size());
		for (TransactionRecordValue recordValue : recordValues) {
			recordValue.writeTransactionValue(dos);
		}
		byte[] bytes = bos.toByteArray();
		currentPosition += bytes.length;
		outputStream.write(bytes);
	}

	public List<Map<String, Object>> readRecordTransactions(int id) throws IOException {
		long position = positionIndex.getValue(id);
		List<Map<String, Object>> changeMap = new ArrayList<>();
		if (position <= 0) {
			return changeMap;
		}
		RandomAccessFile randomAccessFile = new RandomAccessFile(transactionFile, "r");
		FileChannel channel = randomAccessFile.getChannel();
		channel.position(position);

		return changeMap;
	}

	private Map<String, Object> readRecordTransaction(int id, long position) {
		return null;
	}

	public void close() {
		try {
			outputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void drop() {
		try {
			outputStream.close();
			transactionFile.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
