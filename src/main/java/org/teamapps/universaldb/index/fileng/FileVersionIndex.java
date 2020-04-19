package org.teamapps.universaldb.index.fileng;

import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.binary.BinaryIndex;
import org.teamapps.universaldb.index.numeric.ShortIndex;

public class FileVersionIndex {

	private final ShortIndex versionIndex;
	private final BinaryIndex versionDataIndex;

	public FileVersionIndex(String name, TableIndex table) {
		versionIndex = new ShortIndex(name + "-file-version", table);
		versionDataIndex = new BinaryIndex(name + "-file-version-data", table, false);
	}

	/*
		VersionData:
			on version > 0:
			short: version,
	 */

	public short addFile(int id, String oldHash, String oldFileName, long size) {
		short version = getVersion(id);
		versionIndex.setValue(id, ++version);
		if (version > 0) {

		}
		return version;
	}

	public short getVersion(int id) {
		return versionIndex.getValue(id);
	}

}
