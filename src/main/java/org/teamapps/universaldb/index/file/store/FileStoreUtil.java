/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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
package org.teamapps.universaldb.index.file.store;


import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.HexFormat;

public class FileStoreUtil {
	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
	private static final String MAIN_PARTITION_PREFIX = "mp";
	private static final String SECONDARY_PARTITION_PREFIX = "sp";
	private static final String NAME_PREFIX = "c";
	private static final String FILE_SUFFIX = ".cfs";


	public static boolean exists(File basePath, String hash, long length) {
		return getPath(basePath, hash, length).exists();
	}

	public static boolean checkFile(File basePath, String hash, long length, boolean fullCheck) throws Exception {
		File file = getPath(basePath, hash, length);
		if (file.exists()) {
			if (file.length() != length) {
				return false;
			}
			return !fullCheck || createFileHash(file).equalsIgnoreCase(hash);
		} else {
			return false;
		}
	}

	public static File getPath(File basePath, String hash, long length) {
		return new File(basePath, getPrimaryFolder(hash) + "/" + getSecondaryFolder(hash) + "/" + getStoreFileName(hash, length));
	}

	public static File getPath(File basePath, String hash, long length, boolean create) {
		File path = getPath(basePath, hash, length);
		if (create && !path.getParentFile().exists()) {
			path.getParentFile().mkdirs();
		}
		return path;
	}

	public static int getVirtualPartition(String hash) {
		return Integer.parseInt(hash.substring(1, 3), 16);
	}

	public static String getPrimaryFolder(String hash) {
		return MAIN_PARTITION_PREFIX + hash.toLowerCase().charAt(0);
	}

	public static String getSecondaryFolder(String hash) {
		return SECONDARY_PARTITION_PREFIX + hash.toLowerCase().charAt(1);
	}

	public static String getStoreFileName(String hash, long length) {
		return (NAME_PREFIX + hash.toLowerCase() + Long.toString(length, 16) + FILE_SUFFIX).toLowerCase();
	}

	public static long getLengthOfStoreFile(String fileName) {
		return Long.parseLong(fileName.substring(fileName.length() - FILE_SUFFIX.length() - 2));
	}

	public static String createFileHash(File file) throws Exception {
		byte[] buffer = new byte[8192];
		int count;
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
		while ((count = bis.read(buffer)) > 0) {
			digest.update(buffer, 0, count);
		}
		bis.close();
		byte[] hash = digest.digest();
		return bytesToHex(hash);
	}

	public static File createTempFile() throws IOException {
		return Files.createTempFile("tmp", ".tmp").toFile();
	}

	public static String encryptFile(File file, String hash, File encryptedFile) throws Exception {
		Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		byte[] iv = new byte[16];
		byte[] key = HexUtil.hexToBytes(hash);
		System.arraycopy(key, 0, iv, 0, iv.length);
		SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
		IvParameterSpec ivSpec = new IvParameterSpec(iv);
		cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
		DigestOutputStream digestOutputStream = new DigestOutputStream(new BufferedOutputStream(new FileOutputStream(encryptedFile)), digest);
		CipherOutputStream cipherOutputStream = new CipherOutputStream(digestOutputStream, cipher);
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
		byte[] buf = new byte[8096];
		int read;
		while ((read = bis.read(buf)) >= 0) {
			cipherOutputStream.write(buf, 0, read);
		}
		cipherOutputStream.close();
		bis.close();
		return HexUtil.bytesToHex(digest.digest());
	}

	public static File decryptFile(File file, String keyHash) throws Exception{
		File outputFile = createTempFile();
		return decryptFile(file, keyHash, outputFile);
	}

	public static File decryptFile(File file, String keyHash, File outputFile) throws Exception {
		Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		byte[] iv = new byte[16];
		byte[] key = HexUtil.hexToBytes(keyHash);
		System.arraycopy(key, 0, iv, 0, iv.length);
		SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
		IvParameterSpec ivSpec = new IvParameterSpec(iv);
		cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
		CipherInputStream cipherInputStream = new CipherInputStream(new BufferedInputStream(new FileInputStream(file)), cipher);
		DigestInputStream digestInputStream = new DigestInputStream(cipherInputStream, digest);
		BufferedInputStream bis = new BufferedInputStream(digestInputStream);
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile));
		byte[] buf = new byte[8096];
		int numRead = 0;
		while ((numRead = bis.read(buf)) >= 0) {
			bos.write(buf, 0, numRead);
		}
		bos.close();
		bis.close();
		String decryptedFileHash = HexUtil.bytesToHex(digest.digest());
		if (!keyHash.equalsIgnoreCase(decryptedFileHash)) {
			throw new Exception("Error - wrong file hash");
		}
		return outputFile;
	}

	public static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}

	public static byte[] hexToBytes(String hex) {
		return HexFormat.of().parseHex(hex);
	}

	public static String bytesToHex2(byte[] bytes) {
		return HexFormat.of().formatHex(bytes);
	}
}
