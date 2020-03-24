package org.teamapps.universaldb.distribute;

public class ClusterSetConfig {

	private final String zookeeperConfig;
	private final String kafkaConfig;
	private final String sharedSecret;
	private final String topicPrefix;

	public ClusterSetConfig(String zookeeperConfig, String kafkaConfig, String sharedSecret, String topicPrefix) {
		this.zookeeperConfig = zookeeperConfig;
		this.kafkaConfig = kafkaConfig;
		this.sharedSecret = sharedSecret;
		this.topicPrefix = topicPrefix;
	}

	public String getZookeeperConfig() {
		return zookeeperConfig;
	}

	public String getKafkaConfig() {
		return kafkaConfig;
	}

	public String getSharedSecret() {
		return sharedSecret;
	}

	public String getTopicPrefix() {
		return topicPrefix;
	}
}
