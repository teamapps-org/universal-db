/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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
package org.teamapps.universaldb.distribute;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PacketDataMingling {

	private static MessageDigest md;
	private static Charset UTF8 = Charset.forName("UTF8");

	static {
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}


	public synchronized static byte[] getMD5(String value) {
		md.reset();
		md.update(value.getBytes(UTF8));
		return md.digest();
	}


	public static byte[] mingle(byte[] bytes, String sharedSecret, long salt) {
		if (bytes == null) return null;
		byte[] key = getMD5(salt + sharedSecret);
		return mingle(bytes, key);
	}

	public static byte[] mingle(byte[] bytes, byte[] key) {
		byte[] result = new byte[bytes.length];
		final int[] S;
		int is, js;
		S = new int[256];
		for (int i = 0; i < 256; i++) {
			S[i] = i;
		}
		for (int i = 0, j = 0, ki = 0; i < 256; i++) {
			int Si = S[i];
			j = (j + Si + key[ki]) & 0xff;
			S[i] = S[j];
			S[j] = Si;
			ki++;
			if (ki == key.length) {
				ki = 0;
			}
		}
		is = 0;
		js = 0;
		int inLen = bytes.length;
		int outOfs = 0;
		int inOfs = 0;
		while (inLen-- > 0) {
			is = (is + 1) & 0xff;
			int Si = S[is];
			js = (js + Si) & 0xff;
			int Sj = S[js];
			S[is] = Sj;
			S[js] = Si;
			result[outOfs++] = (byte)(bytes[inOfs++] ^ S[(Si + Sj) & 0xff]);
		}
		return result;
	}

}
