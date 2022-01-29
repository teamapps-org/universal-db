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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.Properties;

public class ClusterInfo {

	private final ClusterSetConfig clusterConfig;

	public ClusterInfo(ClusterSetConfig clusterConfig) {
		this.clusterConfig = clusterConfig;
		Properties properties = new Properties();
		properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterConfig.getKafkaConfig());
		AdminClient adminClient = KafkaAdminClient.create(properties);
		Map<MetricName, ? extends Metric> metrics = adminClient.metrics();
		printMetrics(metrics);
	}


	public static void printMetrics(Map<MetricName, ? extends Metric> metrics) {
		metrics.entrySet().forEach(entry -> {
			System.out.println(entry.getKey() + ":" + entry.getValue().metricValue());
		});
	}
}
