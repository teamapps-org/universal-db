/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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
package org.teamapps.universaldb.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DataStreamUtil {

	public static void writeStringWithLengthHeader(DataOutputStream dataOutputStream, String value) throws IOException {
		if (value == null || value.isEmpty()) {
			dataOutputStream.writeInt(0);
		} else {
			byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
			dataOutputStream.writeInt(bytes.length);
			dataOutputStream.write(bytes);
		}
	}

	public static String readStringWithLengthHeader(DataInputStream dataInputStream) throws IOException {
		int length = dataInputStream.readInt();
		if (length == 0) {
			return null;
		}
		byte[] stringBytes = new byte[length];
		dataInputStream.read(stringBytes);
		return new String(stringBytes, StandardCharsets.UTF_8);
	}

	public static void writeByteArrayWithLengthHeader(DataOutputStream dataOutputStream, byte[] bytes) throws IOException {
		dataOutputStream.writeInt(bytes.length);
		dataOutputStream.write(bytes);
	}

	public static byte[] readByteArrayWithLengthHeader(DataInputStream dataInputStream) throws IOException {
		int length = dataInputStream.readInt();
		byte[] bytes = new byte[length];
		dataInputStream.read(bytes);
		return bytes;
	}
}
