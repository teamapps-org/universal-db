/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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
import org.teamapps.universaldb.index.filelegacy.FileUtil;
import org.teamapps.universaldb.index.translation.TranslatableText;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class CollectionTextSearchIndex {

	public static final String ID = "id";

	private String name;
	private File dir;
	private IndexWriter writer;
	private Analyzer queryAnalyzer;
	private StringField idSearchField;
	private NumericDocValuesField idField;
	private FieldType fieldType;

	public CollectionTextSearchIndex(File path, String name) {
		try {
			this.name = name;
			dir = new File(path, name);
			Directory directory = FSDirectory.open(dir.toPath());

			Analyzer analyzer = new StandardAnalyzer();
			queryAnalyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
			iwc.setRAMBufferSizeMB(32);
			writer = new IndexWriter(directory, iwc);

			idSearchField = new StringField(ID, "", Field.Store.NO);
			idField = new NumericDocValuesField(ID, 0);
			fieldType = SearchIndexUtil.createIndexFieldType();

			Runtime.getRuntime().addShutdownHook(new Thread(() -> commit(true)));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setRecordValues(int id, List<FullTextIndexValue> values, boolean update) {
		try {
			idSearchField.setStringValue("" + id);
			idField.setLongValue(id);
			Document doc = new Document();
			doc.add(idSearchField);
			doc.add(idField);
			for (FullTextIndexValue fullTextIndexValue : values) {
				if (fullTextIndexValue.isTranslatableText()) {
					TranslatableText translatableText = fullTextIndexValue.getTranslatableText();
					Map<String, String> translationMap = translatableText.getTranslationMap();
					for (String language : translationMap.keySet()) {
						String value = translationMap.get(language) != null ? translationMap.get(language) : "";
						Field field = new Field(fullTextIndexValue.getFieldName() + "_" + language, value, fieldType);
						doc.add(field);
					}
					Field field = new Field(fullTextIndexValue.getFieldName(), translatableText.getText(), fieldType);
					doc.add(field);
				} else {
					Field field = new Field(fullTextIndexValue.getFieldName(), fullTextIndexValue.getValueNonNull(), fieldType);
					doc.add(field);
				}
			}
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

	public void setFileContent(int id, String fieldName, String content, boolean update) {
		try {
			idSearchField.setStringValue(id + "-" + fieldName);
			idField.setLongValue(id);
			Document doc = new Document();
			doc.add(idSearchField);
			doc.add(idField);
			doc.add(new Field(fieldName, content, fieldType));
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

	public void delete(int id, List<String> fileFieldNames) {
		try {
			Term term = new Term(ID, "" + id);
			writer.deleteDocuments(term);

			if (fileFieldNames != null && !fileFieldNames.isEmpty()) {
				for (String name : fileFieldNames) {
					Term fileFieldTerm = new Term(ID, id + "-" + name);
					writer.deleteDocuments(fileFieldTerm);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public BitSet filter(BitSet bitSet, List<TextFieldFilter> filters, boolean andFilter) {
		try {
			if (filters == null || filters.isEmpty()) {
				return bitSet;
			}
			DirectoryReader reader = DirectoryReader.open(writer, false, false);
			IndexSearcher searcher = new IndexSearcher(reader);
			SearchCollector collector = new SearchCollector();

			BooleanClause.Occur occur = andFilter ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;

			BooleanQuery.Builder fieldQueries = new BooleanQuery.Builder();
			for (TextFieldFilter filter : filters) {
				if (filter.isTranslatableField()) {
					BooleanQuery.Builder translatableQueries = new BooleanQuery.Builder();
					Query originalLanguage = SearchIndexUtil.createQuery(filter.getFilterType(), filter.getFieldName(), filter.getValue(), queryAnalyzer);
					translatableQueries.add(originalLanguage, BooleanClause.Occur.SHOULD);
					for (String language : filter.getRankedLanguages()) {
						Query query = SearchIndexUtil.createQuery(filter.getFilterType(), filter.getFieldName() + "_" + language, filter.getValue(), queryAnalyzer);
						translatableQueries.add(query, BooleanClause.Occur.SHOULD);
					}
					fieldQueries.add(translatableQueries.build(), occur);
				} else {
					Query query = SearchIndexUtil.createQuery(filter.getFilterType(), filter.getFieldName(), filter.getValue(), queryAnalyzer);
					fieldQueries.add(query, occur);
				}
			}

			BooleanQuery query = fieldQueries.build();
			searcher.search(query, collector);
			BitSet resultIds = collector.getResultIds();
			resultIds.and(bitSet);
			reader.close();
			return resultIds;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


	public synchronized void commit(boolean close) {
		try {
			if (writer != null && writer.isOpen()) {
				writer.commit();
				if (close) {
					writer.close();
					writer = null;
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public int getMaxDoc() {
		if (writer == null) {
			return -1;
		} else {
			return writer.getDocStats().maxDoc;
		}
	}

	public void deleteAllDocuments() throws IOException {
		writer.deleteAll();
		writer.commit();
	}

	public void drop() {
		commit(true);
		FileUtil.deleteFileRecursive(dir);
	}

}
