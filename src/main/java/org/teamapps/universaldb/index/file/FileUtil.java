/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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


import org.apache.commons.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class FileUtil {

	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
	private static final int MAX_CHARACTERS_CONTENT = 75_000;

	public static void compressAndEncrypt(File src, File dst, String password) throws Exception {
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dst));
		SecureRandom secureRandom = new SecureRandom();
		byte[] iv = new byte[16];
		secureRandom.nextBytes(iv);
		out.write(iv);
		out.flush();

		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
		SecretKeySpec keySpec = new SecretKeySpec(createHashArray(password), "AES");
		IvParameterSpec ivSpec = new IvParameterSpec(iv);
		cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
		CipherOutputStream cipherOutputStream = new CipherOutputStream(out, cipher);
		DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(new BufferedOutputStream(cipherOutputStream));

		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(src));
		byte[] buf = new byte[8096];
		int read = 0;
		while ((read = bis.read(buf)) >= 0) {
			deflaterOutputStream.write(buf, 0, read);
		}
		deflaterOutputStream.flush();
		cipherOutputStream.flush();
		out.flush();
		deflaterOutputStream.close();
		cipherOutputStream.close();
		out.close();
	}

	public static void decryptAndDecompress(File src, File dst, String password) throws Exception {
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(src));
		byte[] iv = new byte[16];
		IOUtils.readFully(in, iv);
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
		SecretKeySpec keySpec = new SecretKeySpec(createHashArray(password), "AES");
		IvParameterSpec ivSpec = new IvParameterSpec(iv);
		cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
		CipherInputStream cipherInputStream = new CipherInputStream(in, cipher);
		InflaterInputStream inflaterInputStream = new InflaterInputStream(new BufferedInputStream(cipherInputStream));

		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(dst));
		byte[] buf = new byte[8096];
		int numRead = 0;
		while ((numRead = inflaterInputStream.read(buf)) >= 0) {
			bos.write(buf, 0, numRead);
		}
		bos.close();
		inflaterInputStream.close();
	}


	public static FileMetaData parseFileMetaData(File file, FileMetaData metaData) {
		try {
			if (file == null || !file.exists()) {
				return null;
			}
			BodyContentHandler handler = new BodyContentHandler(MAX_CHARACTERS_CONTENT);
			Metadata meta = new Metadata();
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			Parser parser = new AutoDetectParser();
			try {
				parser.parse(bis, handler, meta, new ParseContext());
			} catch (SAXException e) {
				if (!e.getClass().toString().contains("WriteLimitReachedException")) {
					throw e;
				}
			}
			String[] propertyNames = meta.names();
			Arrays.sort(propertyNames);
			if (metaData == null) {
				metaData = new FileMetaData(file.getName(), file.length());
			}
			for (String name : propertyNames) {
				String value = meta.get(name);
				metaData.addMetaDataEntry(name, value);
				if (name.equalsIgnoreCase("Content-Type")) {
					metaData.setMimeType(value);
				}
			}
			metaData.setTextContent(handler.toString());
			//String contentLanguage = getFileContentLanguage(metaData.getTextContent());
			//metaData.setLanguage(contentLanguage);
			String fileHash = FileUtil.createFileHash(file);
			metaData.setHash(fileHash);
			return metaData;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String createFileHash(File file) {
		try {
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String createHash(String... values) {
		try {
			String value = Arrays.stream(values).collect(Collectors.joining("."));
			byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(bytes);
			return bytesToHex(hash);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Missing message digest:" + e.getMessage());
		}
	}

	private static byte[] createHashArray(String value) throws NoSuchAlgorithmException {
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		return digest.digest(bytes);
	}

	private static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}

	public static void deleteFileRecursive(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				deleteFileRecursive(f);
			}
			try {
				file.delete();
			} catch (Throwable e) {
				e.printStackTrace();
			}
		} else {
			try {
				file.delete();
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

}
