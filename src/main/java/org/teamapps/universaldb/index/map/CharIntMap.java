package org.teamapps.universaldb.index.map;

import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;

public class CharIntMap {

	private final File path;
	private final String name;
	private final ChronicleMap<CharSequence, Integer> map;


	public CharIntMap(File path, String name, int maxEntries) throws IOException {
		this.path = path;
		this.name = name;
		File file = new File(path, name + ".idm");
		map = ChronicleMap
				.of(CharSequence.class, Integer.class)
				.name(name)
				.entries(maxEntries)
				.averageKeySize(12)
				.createPersistedTo(file);
	}

	public void put(String key, int value) {
		map.put(key, value);
	}

	public Integer get(String key) {
		return map.get(key);
	}

	public void close() {
		map.close();
	}


}
