/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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
package org.teamapps.universaldb.index.translation;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public class BinaryTranslatedText {

	private final String originalLanguage;
	private final Map<String, String> map = new LinkedHashMap<>();

	public BinaryTranslatedText(String text, String originalLanguage) {
		this.originalLanguage = originalLanguage;
		map.put(originalLanguage, text);
	}

	public BinaryTranslatedText(DataInputStream dis) throws IOException {
		originalLanguage = readShortString(dis);
		int size = dis.readShort();
		for (int i = 0; i < size; i++) {
			String language = readShortString(dis);
			String text = readString(dis);
			map.put(language, text);
		}
	}

	public BinaryTranslatedText(byte[] bytes) throws IOException {
		this(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	public BinaryTranslatedText setTranslation(String text, String language) {
		map.put(language, text);
		return this;
	}

	public String getText() {
		return map.get(originalLanguage);
	}

	public String getText(String language) {
		return map.get(language);
	}

	public byte[] getEncodedValue() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		writeShortString(originalLanguage, dos);
		dos.writeShort(map.size());
		for (Map.Entry<String, String> entry : map.entrySet()) {
			writeShortString(entry.getKey(), dos);
			writeString(entry.getValue(), dos);
		}
		dos.close();
		return bos.toByteArray();
	}

	private void writeShortString(String text, DataOutputStream dos) throws IOException {
		dos.writeByte(text.length());
		dos.write(text.getBytes(StandardCharsets.UTF_8));
	}

	private void writeString(String text, DataOutputStream dos) throws IOException {
		dos.writeInt(text.length());
		dos.write(text.getBytes(StandardCharsets.UTF_8));
	}

	private String readShortString(DataInputStream dis) throws IOException {
		int len = dis.readUnsignedByte();
		byte[] bytes = new byte[len];
		dis.readFully(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	private String readString(DataInputStream dis) throws IOException {
		int len = dis.readInt();
		byte[] bytes = new byte[len];
		dis.readFully(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}
}
