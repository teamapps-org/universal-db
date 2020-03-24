package org.teamapps.universaldb.distribute;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	private long packetKey;
	private final Map<TransactionMessageKey, TransactionExecutionResult> transactionMap = new ConcurrentHashMap<>();

	public TransactionWriter(String brokerConfig,
							 String clientId,
							 String sharedSecret,
							 String topicPrefix
	) {
		this.clientId = clientId;
		this.sharedSecret = sharedSecret;
		this.topic = topicPrefix + "-" + UNRESOLVED_SUFFIX;
		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConfig);
		producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		producer = new KafkaProducer<>(producerProps);
	}

	public TransactionExecutionResult writeTransaction(ClusterTransaction transaction) throws IOException {
		TransactionRequest request = transaction.createRequest();
		TransactionPacket packet = request.getPacket();
		long key = getNextKey();
		byte[] bytes = PacketDataMingling.mingle(packet.writePacketBytes(), sharedSecret, key);
		TransactionMessageKey messageKey = new TransactionMessageKey(TransactionMessageType.TRANSACTION, clientId, key);
		TransactionExecutionResult result = new TransactionExecutionResult();
		transactionMap.put(messageKey, result);
		producer.send(new ProducerRecord<>(topic, messageKey.getBytes(), bytes), this);
		return result;
	}

	private long getNextKey() {
		return ++packetKey;
	}

	public Map<TransactionMessageKey, TransactionExecutionResult> getTransactionMap() {
		return transactionMap;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {

	}
}
