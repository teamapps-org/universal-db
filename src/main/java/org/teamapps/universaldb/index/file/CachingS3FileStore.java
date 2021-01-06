/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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
package org.teamapps.universaldb.index.file;

import io.minio.MinioClient;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class CachingS3FileStore extends AbstractFileStore {

	private final File path;
	private final String bucketName;
	private final String url;
	private final String accessKey;
	private final String secretKey;
	private final FileCache fileCache;
	private final File tempDir;
	private MinioClient minioClient;
	private boolean cachingActive = true;


	public CachingS3FileStore(File tempPath, String bucketName, String url, String accessKey, String secretKey) {
		this.path = tempPath;
		this.bucketName = bucketName;
		this.url = url;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.fileCache = new FileCache(tempPath);
		this.tempDir = new File(tempPath, "temp");
		tempDir.mkdir();
		connect();
	}

	private void connect() {
		try {
			minioClient = new MinioClient(url, accessKey, secretKey);
			createBucket(bucketName);
		} catch (InvalidEndpointException | InvalidPortException e) {
			e.printStackTrace();
		}
	}

	public boolean createBucket(String bucket) {
		try {
			if (!minioClient.bucketExists(bucket)) {
				minioClient.makeBucket(bucket);
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public File getFile(String path, String uuid, String hash) {
		try {
			String filePath = createFilePath(path, uuid);
			File file = null;
			if (cachingActive) {
				fileCache.getFile(filePath);
				if (file != null && file.exists()) {
					return file;
				}
			}
			File tempFile = Files.createTempFile(tempDir.toPath(), "temp", ".bin").toFile();
			minioClient.statObject(bucketName, filePath);
			minioClient.getObject(bucketName, filePath, tempFile.getPath());
			File cacheFile = cachingActive ? fileCache.createCacheFilePath(filePath) : File.createTempFile("temp", ".bin");
			FileUtil.decryptAndDecompress(tempFile, cacheFile, hash);
			if (cachingActive) {
				fileCache.putFinishedCacheFile(cacheFile, filePath);
			}
			tempFile.delete();
			return cacheFile;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void setFile(String path, String uuid, String hash, File file) {
		try {
			String filePath = createFilePath(path, uuid);
			if (cachingActive) {
				fileCache.addFile(file, filePath);
			}
			File tempFile = Files.createTempFile(tempDir.toPath(), "temp", ".bin").toFile();
			FileUtil.compressAndEncrypt(file, tempFile, hash);
			minioClient.putObject(bucketName, filePath, tempFile.getPath(), tempFile.length(), null, null, "application/octet-stream");
			tempFile.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void removeFile(String path, String uuid) {
		try {
			minioClient.removeObject(bucketName, createFilePath(path, uuid));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean fileExists(String path, String uuid) {
		try {
			minioClient.statObject(bucketName, createFilePath(path, uuid));
			return true;
		} catch (Exception ignore) {}
		return false;
	}

	private String createFilePath(String path, String uuid) {
		return path + "/" + uuid + ".bin";
	}

	public boolean isCachingActive() {
		return cachingActive;
	}

	public void setCachingActive(boolean cachingActive) {
		this.cachingActive = cachingActive;
	}
}
