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
package org.teamapps.universaldb.model;

import com.ibm.icu.text.Transliterator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NamingUtils {

	private static Transliterator TRANSLITERATOR = Transliterator.getInstance("Any-Latin; nfd; [:nonspacing mark:] remove; nfc");


	public static void checkName(String name, String title) {
		if (!name.equals(title) && !name.equals(createName(name))) {
			throw new RuntimeException("Element name with invalid characters:" + name + ", should be:" + createName(name));
		}
	}

	public static String createTitle(String s) {
		if (isConstant(s)) {
			List<String> tokens = tokenize(s);
			return firstUpperCase(tokens.stream().map(String::toLowerCase).collect(Collectors.joining(" ")));
		} else if (isCamelCase(s)) {
			return createTitleFromCamelCase(s);
		} else {
			s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ").trim().replaceAll(" +", " ");
			return firstUpperCase(s);
		}
	}

	public static String createName(String s) {
		if (isConstant(s)) {
			s = s.toLowerCase();
		}
		List<String> tokens = tokenize(s);
		String name = tokens.stream()
				.map(v -> TRANSLITERATOR.transliterate(v))
				.map(String::strip)
				.filter(v -> !v.isBlank())
				.map(NamingUtils::firstUpperCase)
				.collect(Collectors.joining());
		return firstLowerCase(name);
	}

	public static List<String> tokenize(String s) {
		s = s
				.replace("'", "")
				.replace("\"", "")
				.replace("\n", " ")
				.replace("\r", " ")
				.replace("\t", " ")
				.trim()
				.replaceAll(" +", " ");
		String[] parts = s.split("[- ,;_()\\[\\]]");
		return Arrays.asList(parts);
	}

	public static boolean isConstant(String s) {
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c != '_' && !Character.isUpperCase(c) && !Character.isDigit(c)) {
				return false;
			}
		}
		return true;
	}

	public static boolean isCamelCase(String s) {
		int countUpper = 0;
		int countLower = 0;
		boolean firstUpper = Character.isUpperCase(s.charAt(0));
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c == ' ' || c == '_') {
				return false;
			}
			if (Character.isUpperCase(c)) {
				countUpper++;
			} else {
				countLower++;
			}
		}
		countUpper = firstUpper ? countUpper - 1 : countUpper;
		return countUpper > 0 && countLower > 0;
	}


	private static void test(String s) {
		System.out.println(createName(s) + ": " + createTitle(s));
	}

	public static String removeNonAnsi(String s) {
		if (s == null) return null;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			int code = c;
			if (c == '_' || c == ' ') {
				sb.append(c);
			} else if (code > 64 && code < 91) {
				sb.append(c);
			} else if (code > 96 && code < 123) {
				sb.append(c);
			} else if (code > 47 && code < 58) {
				sb.append(c);
			}
		}
		return sb.toString();
	}

	public static String firstUpperCase(String s) {
		return s.substring(0, 1).toUpperCase() + s.substring(1);
	}

	public static String firstLowerCase(String s) {
		return s.substring(0, 1).toLowerCase() + s.substring(1);
	}

	public static String createConstantName(String s) {
		if (isConstant(s)) {
			return s;
		} else {
			return s.replace(" ", "_").replaceAll("(.)(\\p{Upper})", "$1_$2").toUpperCase();
		}
	}

	public static String createTitleFromCamelCase(String s) {
		StringBuilder sb = new StringBuilder();
		boolean lastUpperCase = false;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (i < 3) {
				sb.append(c);
			} else {
				if (Character.isUpperCase(c)) {
					if (!lastUpperCase) {
						sb.append(" ");
					}
					lastUpperCase = true;
				} else {
					lastUpperCase = false;
				}
				sb.append(c);
			}
		}
		return firstUpperCase(sb.toString().toLowerCase());
	}

}
