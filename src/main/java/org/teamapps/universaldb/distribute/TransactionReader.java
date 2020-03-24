package org.teamapps.universaldb.distribute;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.index.DataBaseMapper;
import org.teamapps.universaldb.transaction.ClusterTransaction;
import org.teamapps.universaldb.transaction.TransactionIdProvider;
import org.teamapps.universaldb.transaction.TransactionPacket;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TransactionReader {

	public static final String RESOLVED_SUFFIX = "resolved";
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private final String clientId;
	private final String sharedSecret;
	private final DataBaseMapper dataBaseMapper;
	private final Consumer<byte[], byte[]> consumer;
	private final String topic;
	private final TopicPartition topicPartition;
	private final Map<TransactionMessageKey, TransactionExecutionResult> transactionMap;
	private final TransactionIdProvider transactionIdProvider;

	public TransactionReader(String brokerConfig,
							 String clientId,
							 String groupId,
							 String sharedSecret,
							 String topicPrefix,
							 DataBaseMapper dataBaseMapper,
							 Map<TransactionMessageKey, TransactionExecutionResult> transactionMap,
							 TransactionIdProvider transactionIdProvider
	) {
		this.clientId = clientId;
		this.sharedSecret = sharedSecret;
		this.dataBaseMapper = dataBaseMapper;
		this.transactionMap = transactionMap;
		this.transactionIdProvider = transactionIdProvider;
		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConfig);
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupId);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10_000);
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //"latest"

		topic = topicPrefix + "-" + RESOLVED_SUFFIX;
		topicPartition = new TopicPartition(topic, 0);
		consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(Collections.singletonList(topic));
		//consumer.seek(topicPartition, 0);
	}

	private void start() {
		while(true) {
			try {
				consume();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void consume() throws IOException {
		ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofSeconds(1));
		List<ConsumerRecord<byte[], byte[]>> records = consumerRecords.records(topicPartition);
		for (ConsumerRecord<byte[], byte[]> record : records) {
			TransactionMessageKey messageKey = new TransactionMessageKey(record.key());
			byte[] value = record.value();
			byte[] bytes = PacketDataMingling.mingle(value, sharedSecret, messageKey.getLocalKey());

			TransactionPacket transactionPacket = new TransactionPacket(bytes);
			ClusterTransaction transaction = new ClusterTransaction(transactionPacket, dataBaseMapper);

			if (transaction.getTransactionId() == transactionIdProvider.getLastCommittedTransactionId() + 1) {
				commitTransactions(transaction, messageKey);
			} else {
				logger.warn("Transaction with wrong transaction id! Expected id:" + (transactionIdProvider.getLastCommittedTransactionId() + 1) + ", actual id:" + transaction.getTransactionId() + ", key:");
			}
		}
	}

	private void commitTransactions(ClusterTransaction transaction, TransactionMessageKey messageKey) {
		if (!clientId.equals(messageKey.getHeadClientId())) {
			transaction.executeResolvedTransaction();
		}
		if (messageKey.getClientId().equals(clientId)) {
			TransactionExecutionResult executionResult = transactionMap.remove(messageKey);
			executionResult.handleSuccess(transaction.getRecordIdByCorrelationId());
		}
	}


}
