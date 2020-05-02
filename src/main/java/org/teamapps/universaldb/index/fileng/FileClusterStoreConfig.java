package org.teamapps.universaldb.index.fileng;

public class FileClusterStoreConfig {

	private final String fileSecret;
	private final String url;
	private final String accessKey;
	private final String secretKey;

	public FileClusterStoreConfig(String fileSecret, String url, String accessKey, String secretKey) {
		this.fileSecret = fileSecret;
		this.url = url;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
	}

	public String getFileSecret() {
		return fileSecret;
	}

	public String getUrl() {
		return url;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}
}
