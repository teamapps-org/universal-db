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
package org.teamapps.universaldb.distribute;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.SchemaStats;
import org.teamapps.universaldb.index.DataBaseMapper;
import org.teamapps.universaldb.transaction.ClusterTransaction;
import org.teamapps.universaldb.transaction.TransactionIdHandler;
import org.teamapps.universaldb.transaction.TransactionPacket;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TransactionMaster implements LeaderSelectorListener, Closeable, Callback {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private static final String LEADER_SELECTION_NODE = "/leaderSelection/leader";

	private final String sharedSecret;
	private final ClusterSetConfig clusterConfig;
	private final SchemaStats schemaStats;
	private final DataBaseMapper dataBaseMapper;
	private final TransactionIdHandler transactionIdHandler;
	private final Consumer<byte[], byte[]> consumer;
	private final KafkaProducer<byte[], byte[]> producer;
	private final LeaderSelector leaderSelector;

	private final String consumerTopic;
	private final TopicPartition consumerTopicPartition;
	private final String producerTopic;
	private volatile boolean masterRole;
	private final String masterProducerClientId;
	private long packetKey;

	public TransactionMaster(ClusterSetConfig clusterConfig,
							 SchemaStats schemaStats,
							 DataBaseMapper dataBaseMapper,
							 TransactionIdHandler transactionIdHandler
	) {
		this.clusterConfig = clusterConfig;
		this.sharedSecret = clusterConfig.getSharedSecret();
		this.schemaStats = schemaStats;
		this.dataBaseMapper = dataBaseMapper;
		this.transactionIdHandler = transactionIdHandler;
		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterConfig.getKafkaConfig());
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000_000);
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //"latest"

		logger.info("Start with master transaction offset:" + schemaStats.getMasterTransactionOffset());

		consumerTopic = clusterConfig.getTopicPrefix() + "-" + TransactionWriter.UNRESOLVED_SUFFIX;
		consumerTopicPartition = new TopicPartition(consumerTopic, 0);
		consumer = new KafkaConsumer<>(consumerProps);
		consumer.assign(Collections.singletonList(consumerTopicPartition));
		consumer.seek(consumerTopicPartition, schemaStats.getMasterTransactionOffset());

		masterProducerClientId = schemaStats.getMasterClientId();
		producerTopic = clusterConfig.getTopicPrefix() + "-" + TransactionReader.RESOLVED_SUFFIX;
		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterConfig.getKafkaConfig());
		producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, masterProducerClientId);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		producerProps.put(ProducerConfig.ACKS_CONFIG, "1"); //"all"
		producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
		producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 1_000_000);
		producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10 * 1024 * 1024);
		producer = new KafkaProducer<>(producerProps);

		CuratorFramework client = CuratorFrameworkFactory.newClient(clusterConfig.getZookeeperConfig(), new ExponentialBackoffRetry(1000, 3));
		leaderSelector = new LeaderSelector(client, LEADER_SELECTION_NODE, this);
		client.start();
		leaderSelector.autoRequeue();
		leaderSelector.start();
	}

	@Override
	public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
		if (!connectionState.isConnected()) {
			logger.info("Lost connection to zookeeper");
			masterRole = false;
		}
	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		logger.info("START TRANSACTION MASTER: " + Thread.currentThread().getName());
		if (masterRole) {
			logger.warn("Error: starting master twice!");
			return;
		}
		masterRole = true;

		long unconsumedMessageCount;
		while (masterRole && ((unconsumedMessageCount = getUnconsumedMessageCount()) > 0)) {
			logger.info("Master waiting for worker topics to be consumed:" + unconsumedMessageCount);
			Thread.sleep(1000);
		}

		consumer.seek(consumerTopicPartition, schemaStats.getMasterTransactionOffset());
		while (masterRole) {
			System.out.println("Consume master transaction log...");
			handleMessages();
		}
		logger.info("END TRANSACTION MASTER");
	}

	private long getUnconsumedMessageCount() {
		try {
			Properties consumerProps = new Properties();
			consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterConfig.getKafkaConfig());
			consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

			KafkaConsumer<Object, Object> resolvedTopicConsumer = new KafkaConsumer<>(consumerProps);
			TopicPartition topicPartition = new TopicPartition(clusterConfig.getTopicPrefix() + "-" + TransactionReader.RESOLVED_SUFFIX, 0);
			Map<TopicPartition, Long> endOffsets = resolvedTopicConsumer.endOffsets(Collections.singletonList(topicPartition));

			Long latestOffset = endOffsets.get(topicPartition);
			long unconsumedMessages =  latestOffset -1  - schemaStats.getMasterTransactionOffset();
			resolvedTopicConsumer.close();
			return unconsumedMessages;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return Integer.MAX_VALUE;
	}

	private void handleMessages() throws IOException {
		ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofSeconds(3));
		List<ConsumerRecord<byte[], byte[]>> records = consumerRecords.records(consumerTopicPartition);
		for (ConsumerRecord<byte[], byte[]> record : records) {
			TransactionMessageKey messageKey = new TransactionMessageKey(record.key());
			byte[] value = record.value();
			byte[] bytes = PacketDataMingling.mingle(value, sharedSecret, messageKey.getPacketKey());
			logger.debug("MASTER received new transaction:" + messageKey);

			TransactionPacket transactionPacket = new TransactionPacket(bytes);
			ClusterTransaction transaction = new ClusterTransaction(transactionPacket, dataBaseMapper);

			TransactionMessageKey masterMessageKey = TransactionMessageKey.createFromKey(messageKey, getNextKey(), masterProducerClientId, record.offset());
			transactionPacket = transaction.resolveAndExecuteTransaction(transactionIdHandler, transactionPacket);

			if (transactionPacket != null) {
				logger.debug("MASTER send new transaction:" + masterMessageKey + ", transaction-id:" + transaction.getTransactionId());
				byte[] packetBytes = transactionPacket.writePacketBytes();
				byte[] mingledBytes = PacketDataMingling.mingle(packetBytes, sharedSecret, masterMessageKey.getPacketKey());
				producer.send(new ProducerRecord<>(producerTopic, masterMessageKey.getBytes(), mingledBytes), this);
			} else {
				logger.info("Sending error packet...");
				producer.send(new ProducerRecord<>(producerTopic, masterMessageKey.getBytes(), null));
			}
		}
	}

	@Override
	public void close() throws IOException {
		leaderSelector.close();
	}

	private long getNextKey() {
		return ++packetKey;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception e) {
		if (e != null) {
			logger.warn("Error writing master transaction:" + e.getMessage());
		}
	}
}
