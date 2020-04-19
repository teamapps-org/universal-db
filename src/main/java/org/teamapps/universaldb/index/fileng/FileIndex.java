package org.teamapps.universaldb.index.fileng;

import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.binary.BinaryIndex;
import org.teamapps.universaldb.index.file.FileFilter;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.numeric.LongIndex;
import org.teamapps.universaldb.index.numeric.ShortIndex;
import org.teamapps.universaldb.index.text.CollectionTextSearchIndex;
import org.teamapps.universaldb.index.text.TextIndex;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;

public class FileIndex extends AbstractIndex<FileValue, FileFilter> {

	private final TextIndex hashIndex;
	private final TextIndex fileNameIndex;
	private final LongIndex sizeIndex;
	private final FileVersionIndex versionIndex;
	private final CollectionTextSearchIndex fullTextIndex;
	private final String filePath;
	private final boolean indexFileContent;

	public FileIndex(String name, TableIndex table, boolean indexFileContent) {
		super(name, table, indexFileContent ? FullTextIndexingOptions.INDEXED : FullTextIndexingOptions.NOT_INDEXED);
		this.indexFileContent = indexFileContent;
		hashIndex = new TextIndex(name + "-file-hash", table, false);
		fileNameIndex = new TextIndex(name + "-file-name", table, false);
		sizeIndex = new LongIndex(name + "-file-size", table);
		versionIndex = new FileVersionIndex(name, table);
		fullTextIndex = new CollectionTextSearchIndex(getPath(), name);
		filePath = getFQN().replace('.', '/');
	}

	@Override
	public IndexType getType() {
		return IndexType.FILE_NG;
	}

	@Override
	public FileValue getGenericValue(int id) {
		return null;
	}

	@Override
	public void setGenericValue(int id, FileValue value) {

	}

	@Override
	public void removeValue(int id) {

	}

	@Override
	public void writeTransactionValue(FileValue value, DataOutputStream dataOutputStream) throws IOException {

	}

	@Override
	public FileValue readTransactionValue(DataInputStream dataInputStream) throws IOException {
		return null;
	}

	@Override
	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, Locale locale) {
		return null;
	}

	@Override
	public BitSet filter(BitSet records, FileFilter fileFilter) {
		return null;
	}

	@Override
	public void close() {

	}

	@Override
	public void drop() {

	}
}
