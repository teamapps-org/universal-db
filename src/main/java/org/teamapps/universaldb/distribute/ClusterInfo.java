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
