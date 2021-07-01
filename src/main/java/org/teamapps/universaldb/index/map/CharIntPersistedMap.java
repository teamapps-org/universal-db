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
package org.teamapps.universaldb.index.map;

import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;

public class CharIntPersistedMap {

	private static final String MAP_CURRENT_ENTRIES_KEY = "_mapCurrent$Entries#";
	private static final int BASE_ENTRIES = 100_000;
	private final File path;
	private final String name;
	private ChronicleMap<CharSequence, Integer> map;

	private int entries;
	private int maxEntries;
	private int fileIndex;

	public CharIntPersistedMap(File path, String name) {
		this.path = path;
		this.name = name;
		init();
	}

	private void init() {
		int index = getFileIndex();
		if (index < 0) {
			fileIndex = 0;
			maxEntries = getEntries(fileIndex);
			entries = 0;
			map = createMap(getStoreFile(this.fileIndex), maxEntries);
			map.put(MAP_CURRENT_ENTRIES_KEY, entries);
		} else {
			fileIndex = index;
			maxEntries = getEntries(fileIndex);
			map = createMap(getStoreFile(this.fileIndex), maxEntries);
			entries = map.get(MAP_CURRENT_ENTRIES_KEY);
		}
	}

	private void checkSize() {
		if (entries + 1 >= maxEntries) {
			fileIndex++;
			maxEntries = getEntries(fileIndex);
			ChronicleMap<CharSequence, Integer> newMap = createMap(getStoreFile(this.fileIndex), maxEntries);
			copyMap(map, newMap);
			ChronicleMap<CharSequence, Integer> oldMap = map;
			newMap.put(MAP_CURRENT_ENTRIES_KEY, entries);
			map = newMap;
			oldMap.close();
		}
	}

	private int getFileIndex() {
		int index = 0;
		while (getStoreFile(index).exists()) {
			index++;
		}
		return index - 1;
	}

	private int getEntries(int index) {
		return (int) Math.pow(2, index) * BASE_ENTRIES;
	}

	private void copyMap(ChronicleMap<CharSequence, Integer> src, ChronicleMap<CharSequence, Integer> dst) {
		for (CharSequence key : src.keySet()) {
			dst.put(key, src.get(key));
		}
	}

	private ChronicleMap<CharSequence, Integer> createMap(File storeFile, int maxEntries) {
		try {
			ChronicleMap<CharSequence, Integer> map = ChronicleMap
					.of(CharSequence.class, Integer.class)
					.name(name)
					.entries(maxEntries)
					.averageKeySize(30)
					.createPersistedTo(storeFile);
			return map;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		long time = System.currentTimeMillis();
		File path = new File("/Users/mb/dev/test/map/");
		CharIntPersistedMap map = new CharIntPersistedMap(path, "cmap");
		int size = 10_000;
		for (int i = 0; i < size; i++) {
			map.put("" + i, i);
		}
		System.out.println("TIME:" + (System.currentTimeMillis() - time));
		time = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			Integer value = map.get("" + i);
			if (value == null || value != i) {
				System.out.println("Error:" + i + ", val:" + value);
			}
		}
		System.out.println("TIME:" + (System.currentTimeMillis() - time));
		map.close();
	}

	private File getStoreFile(int index) {
		return new File(getPath(), getName() + "-" + index + ".idm");
	}

	public File getPath() {
		return path;
	}

	public String getName() {
		return name;
	}

	public synchronized void put(String key, int value) {
		if (!map.containsKey(key)) {
			entries++;
			checkSize();
			map.put(MAP_CURRENT_ENTRIES_KEY, entries);
		}
		map.put(key, value);
	}

	public Integer get(String key) {
		return map.get(key);
	}

	public void close() {
		map.close();
	}

	public void drop() {
		close();
		map = null;
		for (int i = 0; i <= fileIndex; i++) {
			File storeFile = getStoreFile(i);
			if (storeFile.exists()) {
				storeFile.delete();
			}
		}
	}

}
