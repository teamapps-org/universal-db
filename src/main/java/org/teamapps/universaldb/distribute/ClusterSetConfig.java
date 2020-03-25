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

public class ClusterSetConfig {

	private final String zookeeperConfig;
	private final String kafkaConfig;
	private final String sharedSecret;
	private final String topicPrefix;

	private String producerClientId;
	private String consumerGroupId;
	private String headProducerClientId;
	private String headConsumerGroupId;

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

	public String getProducerClientId() {
		return producerClientId;
	}

	public String getConsumerGroupId() {
		return consumerGroupId;
	}

	public String getHeadProducerClientId() {
		return headProducerClientId;
	}

	public String getHeadConsumerGroupId() {
		return headConsumerGroupId;
	}

	public void setProducerClientId(String producerClientId) {
		this.producerClientId = producerClientId;
	}

	public void setConsumerGroupId(String consumerGroupId) {
		this.consumerGroupId = consumerGroupId;
	}

	public void setHeadProducerClientId(String headProducerClientId) {
		this.headProducerClientId = headProducerClientId;
	}

	public void setHeadConsumerGroupId(String headConsumerGroupId) {
		this.headConsumerGroupId = headConsumerGroupId;
	}
}
