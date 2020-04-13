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
package org.teamapps.universaldb.distribute;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.SchemaStats;
import org.teamapps.universaldb.index.DataBaseMapper;
import org.teamapps.universaldb.transaction.ClusterTransaction;
import org.teamapps.universaldb.transaction.TransactionIdHandler;
import org.teamapps.universaldb.transaction.TransactionPacket;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.*;

public class TransactionReader {

	public static final String RESOLVED_SUFFIX = "resolved";
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private final String clientId;
	private final String masterProducerClientId;
	private final String sharedSecret;
	private final SchemaStats schemaStats;
	private final DataBaseMapper dataBaseMapper;
	private final Consumer<byte[], byte[]> consumer;
	private final String topic;
	private final TopicPartition topicPartition;
	private final Map<Long, TransactionExecutionResult> transactionMap;
	private final TransactionIdHandler transactionIdHandler;

	public TransactionReader(ClusterSetConfig clusterConfig,
							 SchemaStats schemaStats,
							 DataBaseMapper dataBaseMapper,
							 Map<Long, TransactionExecutionResult> transactionMap,
							 TransactionIdHandler transactionIdHandler
	) {
		this.sharedSecret = clusterConfig.getSharedSecret();
		this.schemaStats = schemaStats;
		this.dataBaseMapper = dataBaseMapper;
		this.transactionMap = transactionMap;
		this.transactionIdHandler = transactionIdHandler;
		this.clientId = schemaStats.getClientId();
		this.masterProducerClientId = schemaStats.getMasterClientId();
		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterConfig.getKafkaConfig());
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000_000);
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //"latest"

		logger.info("Start with transaction offset:" + schemaStats.getTransactionOffset());

		topic = clusterConfig.getTopicPrefix() + "-" + RESOLVED_SUFFIX;
		topicPartition = new TopicPartition(topic, 0);
		consumer = new KafkaConsumer<>(consumerProps);
		consumer.assign(Collections.singletonList(topicPartition));
		consumer.seek(topicPartition, schemaStats.getTransactionOffset());
		new Thread(() -> start()).start();
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
			byte[] bytes = PacketDataMingling.mingle(value, sharedSecret, messageKey.getPacketKey());

			TransactionPacket transactionPacket = new TransactionPacket(bytes);
			ClusterTransaction transaction = new ClusterTransaction(transactionPacket, dataBaseMapper);

			if (messageKey.getClientId().equals(clientId)) {
				TransactionExecutionResult executionResult = transactionMap.remove(messageKey.getTransactionKeyOfCallingNode());
				if (executionResult != null) {
					if (value != null) {
						executionResult.handleSuccess(transaction.getRecordIdByCorrelationId());
					} else {
						executionResult.handleError();
					}
				}
			}

			if (!messageKey.getMasterClientId().equals(masterProducerClientId)) {
				if (transaction.getTransactionId() == transactionIdHandler.getLastCommittedTransactionId() + 1) {
					transaction.executeResolvedTransaction(transactionIdHandler);
				} else {
					logger.warn("Transaction with wrong transaction id! Expected id:" + (transactionIdHandler.getLastCommittedTransactionId() + 1) + ", actual id:" + transaction.getTransactionId() + ", key:" + messageKey);
				}
			}

			schemaStats.setTransactionOffset(record.offset() + 1);
			schemaStats.setMasterTransactionOffset(messageKey.getMasterOffset() + 1);

		}
	}

}
