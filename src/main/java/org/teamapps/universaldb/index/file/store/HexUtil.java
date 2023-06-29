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
package org.teamapps.universaldb.index.file.store;

import java.util.HexFormat;

public class HexUtil {

	public static final char[] HEX_ARRAY = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

	public static String bytesToHex(byte[] bytes) {
		char[] chars = new char[bytes.length * 2];
		int v;
		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF;
			chars[j * 2] = HEX_ARRAY[v >>> 4];
			chars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(chars);
	}

	public static byte[] hexToBytes(String hex) {
		return HexFormat.of().parseHex(hex);
	}

	public static String bytesToHex2(byte[] bytes) {
		return HexFormat.of().formatHex(bytes);
	}


}
