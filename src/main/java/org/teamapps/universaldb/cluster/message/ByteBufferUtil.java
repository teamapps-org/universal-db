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
package org.teamapps.universaldb.cluster.message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteBufferUtil {

	public static ByteBuffer create(byte[] bytes) {
		return ByteBuffer.wrap(bytes);
	}

	public static void putBytesWithHeader(byte[] bytes, ByteBuffer buffer) {
		if (bytes == null) {
			buffer.putInt(0);
			return;
		}
		buffer.putInt(bytes.length);
		buffer.put(bytes);
	}

	public static byte[] getBytesWithHeader(ByteBuffer buffer) {
		int len = buffer.getInt();
		if (len == 0) {
			return null;
		}
		byte[] bytes = new byte[len];
		buffer.get(bytes);
		return bytes;
	}

	public static void putStringWithHeader(String value, ByteBuffer buffer) {
		if (value == null) {
			buffer.putInt(0);
			return;
		}
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		buffer.putInt(bytes.length);
		buffer.put(bytes);
	}

	public static String getStringWithHeader(ByteBuffer buffer) {
		int len = buffer.getInt();
		if (len == 0) {
			return null;
		}
		byte[] bytes = new byte[len];
		buffer.get(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}


}
