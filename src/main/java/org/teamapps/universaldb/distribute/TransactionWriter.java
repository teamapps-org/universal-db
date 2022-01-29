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

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.SchemaStats;
import org.teamapps.universaldb.transaction.ClusterTransaction;
import org.teamapps.universaldb.transaction.TransactionPacket;
import org.teamapps.universaldb.transaction.TransactionRequest;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionWriter implements Callback {

	public static final String UNRESOLVED_SUFFIX = "unresolved";
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private final KafkaProducer<byte[], byte[]> producer;
	private final String clientId;
	private final String sharedSecret;
	private final String topic;
	private final SchemaStats schemaStats;
	private long packetKey;
	private final Map<Long, TransactionExecutionResult> transactionMap = new ConcurrentHashMap<>();

	public TransactionWriter(ClusterSetConfig clusterConfig, SchemaStats schemaStats) {
		this.clientId = schemaStats.getClientId();
		this.sharedSecret = clusterConfig.getSharedSecret();
		this.topic = clusterConfig.getTopicPrefix() + "-" + UNRESOLVED_SUFFIX;
		this.schemaStats = schemaStats;
		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterConfig.getKafkaConfig());
		producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		producerProps.put(ProducerConfig.ACKS_CONFIG, "1"); //"all"
		producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 20);
		producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 1_000_000);
		producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10 * 1024 * 1024);
		producer = new KafkaProducer<>(producerProps);
	}

	public TransactionExecutionResult writeTransaction(ClusterTransaction transaction) throws IOException {
		TransactionRequest request = transaction.createRequest();
		TransactionPacket packet = request.getPacket();
		long key = getNextKey();
		byte[] packetBytes = packet.writePacketBytes();
		byte[] bytes = PacketDataMingling.mingle(packetBytes, sharedSecret, key);
		TransactionMessageKey messageKey = new TransactionMessageKey(TransactionMessageType.TRANSACTION, clientId, key);
		TransactionExecutionResult result = new TransactionExecutionResult();
		transactionMap.put(messageKey.getTransactionKeyOfCallingNode(), result);
		producer.send(new ProducerRecord<>(topic, messageKey.getBytes(), bytes), this);
		return result;
	}

	private long getNextKey() {
		return ++packetKey;
	}

	public Map<Long, TransactionExecutionResult> getTransactionMap() {
		return transactionMap;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception e) {
		if (e != null) {
			logger.warn("Error writing transaction:" + e.getMessage());
		}
	}
}
