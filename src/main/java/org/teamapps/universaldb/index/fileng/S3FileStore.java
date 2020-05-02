package org.teamapps.universaldb.index.fileng;

import io.minio.MinioClient;
import io.minio.ObjectStat;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class S3FileStore implements RemoteFileStore {

	private final String bucketName;
	private final String url;
	private final String accessKey;
	private final String secretKey;
	private MinioClient minioClient;

	public S3FileStore(String bucketName, String url, String accessKey, String secretKey) throws Exception {
		this.bucketName = bucketName;
		this.url = url;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		connect();
	}

	private void connect() throws Exception {
		minioClient = new MinioClient(url, accessKey, secretKey);
		createBucket(bucketName);
	}

	public void createBucket(String bucket) throws Exception {
		if (!minioClient.bucketExists(bucket)) {
			minioClient.makeBucket(bucket);
		}
	}

	@Override
	public InputStream getInputStream(String path) throws Exception {
		return minioClient.getObject(bucketName, path);
	}

	@Override
	public File getFile(String path) throws Exception {
		minioClient.statObject(bucketName, path);
		File tempFile = File.createTempFile("temp", ".bin");
		minioClient.getObject(bucketName, path, tempFile.getPath());
		return tempFile;
	}

	@Override
	public void setInputStream(String path, InputStream inputStream, long length) throws Exception {
		minioClient.putObject(bucketName, path, inputStream, length, null, null, "application/octet-stream");
	}

	@Override
	public void setFile(String path, File file) throws Exception {
		minioClient.putObject(bucketName, path, file.getPath(), file.length(), null, null, "application/octet-stream");
	}

	@Override
	public boolean fileExists(String path) {
		try {
			minioClient.statObject(bucketName, path);
			return true;
		} catch (Exception ignore) {
		}
		return false;
	}

	@Override
	public void removeFile(String path) throws Exception {
		minioClient.removeObject(bucketName, path);
	}
}
