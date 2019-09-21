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
package org.teamapps.universaldb.index.text;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.teamapps.universaldb.index.file.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;

public class TextSearchIndex {

	public static final String VALUE = "value";
	public static final String ID = "id";

	private File dir;
	private StringField idSearchField;
	private NumericDocValuesField idField;
	private Field valueField;
	private IndexWriter writer;
	private Analyzer queryAnalyzer;

	public TextSearchIndex(File path, String name) {
		try {
			dir = new File(path, name);
			Directory directory = FSDirectory.open(dir.toPath());
			Analyzer analyzer = new StandardAnalyzer();
			queryAnalyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
			iwc.setRAMBufferSizeMB(12);
			writer = new IndexWriter(directory, iwc);

			idSearchField = new StringField(ID, "", Field.Store.NO);
			idField = new NumericDocValuesField(ID, 0);
			valueField = new Field(VALUE, "", SearchIndexUtil.createIndexFieldType());

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				if (writer != null && writer.isOpen()) {
					try {
						writer.commit();
						writer.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addValue(int id, String value, boolean update) {
		try {
			idSearchField.setStringValue("" + id);
			idField.setLongValue(id);
			valueField.setStringValue(value);
			Document doc = new Document();
			doc.add(idSearchField);
			doc.add(idField);
			doc.add(valueField);
			if (update) {
				Term term = new Term(ID, "" + id);
				writer.updateDocument(term, doc);
			} else {
				writer.addDocument(doc);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void removeValue(int id) {
		try {
			Term term = new Term(ID, "" + id);
			writer.deleteDocuments(term);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public BitSet filter(BitSet bitSet, TextFilter textFilter) {
		try {
			if (textFilter == null) {
				return bitSet;
			}
			DirectoryReader reader = DirectoryReader.open(writer, false, false);
			IndexSearcher searcher = new IndexSearcher(reader);
			SearchCollector collector = new SearchCollector();
			Query query = SearchIndexUtil.createQuery(textFilter.getFilterType(), VALUE, textFilter.getValue(), queryAnalyzer);
			searcher.search(query, collector);
			BitSet resultIds = collector.getResultIds();
			resultIds.and(bitSet);
			return resultIds;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void commit(boolean close) {
		try {
			if (writer != null && writer.isOpen()) {
				writer.commit();
				if (close) {
					writer.close();
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public void drop() {
		commit(true);
		FileUtil.deleteFileRecursive(dir);
	}



}
