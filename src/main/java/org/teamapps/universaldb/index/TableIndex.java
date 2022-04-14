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
package org.teamapps.universaldb.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.TableConfig;
import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.bool.BooleanIndex;
import org.teamapps.universaldb.index.file.FileStore;
import org.teamapps.universaldb.index.numeric.LongIndex;
import org.teamapps.universaldb.index.reference.ReferenceIndex;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.text.CollectionTextSearchIndex;
import org.teamapps.universaldb.index.text.FullTextIndexValue;
import org.teamapps.universaldb.index.text.TextFilter;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.index.translation.TranslatableTextIndex;
import org.teamapps.universaldb.index.versioning.RecordVersioningIndex;
import org.teamapps.universaldb.query.AndFilter;
import org.teamapps.universaldb.query.Filter;
import org.teamapps.universaldb.query.IndexFilter;
import org.teamapps.universaldb.query.OrFilter;
import org.teamapps.universaldb.schema.Column;
import org.teamapps.universaldb.schema.Table;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;

public class TableIndex implements MappedObject {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final DatabaseIndex databaseIndex;
	private final Table table;
	private final String name;
	private final String parentFQN;
	private final File dataPath;
	private final File fullTextIndexPath;
	private final TableConfig tableConfig;
	private boolean keepDeletedRecords;
	private final BooleanIndex records;
	private BooleanIndex deletedRecords;
	private LongIndex transactionIndex;

	private final List<ColumnIndex> columnIndices;
	private final Map<String, ColumnIndex> columnIndexByName;
	private CollectionTextSearchIndex collectionTextSearchIndex;
	private List<String> fileFieldNames;
	private List<TextIndex> textFields;
	private List<TranslatableTextIndex> translatedTextFields;
	private int mappingId;
	private IndexMetaData indexMetaData;
	private RecordVersioningIndex recordVersioningIndex;
	private long lastFullTextIndexCheck;

	public TableIndex(DatabaseIndex database, Table table, TableConfig tableConfig) {
		this(database, database.getFQN(), table, tableConfig);
	}

	public TableIndex(DatabaseIndex databaseIndex, String parentFQN, Table table, TableConfig tableConfig) {
		this.databaseIndex = databaseIndex;
		this.table = table;
		this.name = table.getName();
		this.parentFQN = parentFQN;
		this.dataPath = new File(databaseIndex.getDataPath(), name);
		this.fullTextIndexPath = new File(databaseIndex.getFullTextIndexPath(), name);
		dataPath.mkdir();
		fullTextIndexPath.mkdir();
		records = new BooleanIndex("coll-recs", this, ColumnType.BOOLEAN);
		this.tableConfig = tableConfig;

		columnIndices = new ArrayList<>();
		columnIndexByName = new HashMap<>();

		if (tableConfig.keepDeleted()) {
			keepDeletedRecords = true;
			deletedRecords = new BooleanIndex("coll-del-recs", this, ColumnType.BOOLEAN);
		}
		this.indexMetaData = new IndexMetaData(dataPath, name, getFQN(), 0);
		this.mappingId = indexMetaData.getMappingId();
		this.recordVersioningIndex = new RecordVersioningIndex(this);
		Runtime.getRuntime().addShutdownHook(new Thread(this::close));
	}


	public CollectionTextSearchIndex getCollectionTextSearchIndex() {
		if (collectionTextSearchIndex == null) {
			collectionTextSearchIndex = new CollectionTextSearchIndex(fullTextIndexPath, "coll-text");
		}
		return collectionTextSearchIndex;
	}

	public void checkFullTextIndex() {
		if (collectionTextSearchIndex == null) {
			return;
		}
		if (
				(!records.getValue(0) && getCount() > 0 && (System.currentTimeMillis() - lastFullTextIndexCheck > 300_000)) ||
						getCount() > 0 && collectionTextSearchIndex.getMaxDoc() == 0
		) {
			long time = System.currentTimeMillis();
			logger.warn("RECREATING FULL TEXT INDEX FOR: " + getName() + " (RECORDS:" + getCount() + ", MAX-DOC:" + collectionTextSearchIndex.getMaxDoc() + ")");
			recreateFullTextIndex();
			lastFullTextIndexCheck = System.currentTimeMillis();
			logger.warn("RECREATING FINISHED FOR: " + getName() + " (TIME:" + (System.currentTimeMillis() - time) + ")");
		}
		records.setValue(0, false);
	}

	public void forceFullTextIndexRecreation() {
		logger.warn("FORCED RECREATING FULL TEXT INDEX FOR: " + getName() + " (RECORDS:" + getCount() + ", MAX-DOC:" + collectionTextSearchIndex.getMaxDoc() + ")");
		recreateFullTextIndex();

	}

	private void recreateFullTextIndex() {
		try {
			collectionTextSearchIndex.deleteAllDocuments();
			BitSet bitSet = records.getBitSet();
			for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
				List<FullTextIndexValue> values = new ArrayList<>();
				for (TextIndex textField : getTextFields()) {
					String value = textField.getValue(id);
					if (value != null) {
						values.add(new FullTextIndexValue(textField.getName(), value));
					}
				}
				for (TranslatableTextIndex translatableTextIndex : getTranslatedTextFields()) {
					TranslatableText value = translatableTextIndex.getValue(id);
					if (value != null) {
						values.add(new FullTextIndexValue(translatableTextIndex.getName(), value));
					}
				}
				if (!values.isEmpty()) {
					collectionTextSearchIndex.setRecordValues(id, values, false);
				}
			}
			collectionTextSearchIndex.commit(false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Table getTable() {
		return table;
	}

	public FileStore getFileStore() {
		return databaseIndex.getSchemaIndex().getFileStore();
	}

	public RecordVersioningIndex getRecordVersioningIndex() {
		return recordVersioningIndex;
	}

	public File getDataPath() {
		return dataPath;
	}

	public File getFullTextIndexPath() {
		return fullTextIndexPath;
	}

	public TableConfig getTableConfig() {
		return tableConfig;
	}

	public BitSet getRecords() {
		return records.getBitSet();
	}

	public boolean isStored(int id) {
		return records.getValue(id);
	}

	public int getCount() {
		return records.getCount();
	}

	public BitSet getDeletedRecords() {
		if (!keepDeletedRecords) {
			return null;
		}
		return deletedRecords.getBitSet();
	}

	public int getDeletedRecordsCount() {
		return keepDeletedRecords ? deletedRecords.getCount() : 0;
	}

	public boolean isDeleted(int id) {
		if (deletedRecords != null) {
			return deletedRecords.getValue(id);
		} else {
			return !records.getValue(id);
		}
	}

	public void addIndex(ColumnType type, String name) {
		ColumnIndex column = ColumnIndex.createColumn(this, name, type);
		addIndex(column);
	}

	public void addIndex(ColumnIndex index) {
		columnIndices.add(index);
		columnIndexByName.put(index.getName(), index);
		fileFieldNames = null;
		textFields = null;
	}

	public Filter createFullTextFilter(String query, String... fieldNames) {
		AndFilter andFilter = new AndFilter();
		if (query == null || query.isBlank()) {
			return andFilter;
		}
		String[] terms = query.split(" ");
		for (String term : terms) {
			if (!term.isBlank()) {
				boolean isNegation = term.startsWith("!");
				TextFilter textFilter = parseTextFilter(term);
				Filter fullTextFilter = createFullTextFilter(textFilter, !isNegation, fieldNames);
				andFilter.and(fullTextFilter);
			}
		}
		return andFilter;
	}

	private TextFilter parseTextFilter(String term) {
		boolean negation = false;
		boolean similar = false;
		boolean startsWith = false;
		boolean equals = false;
		if (term.startsWith("!")) {
			negation = true;
			term = term.substring(1);
		}
		if (term.endsWith("+")) {
			similar = true;
			term = term.substring(0, term.length() - 1);
		}
		if (term.endsWith("*")) {
			startsWith = true;
			term = term.substring(0, term.length() - 1);
		}
		if (term.contains("\"")) {
			equals = true;
			term = term.replace("\"", "");
		}
		if (equals) {
			return negation ? TextFilter.termEqualsFilter(term) : TextFilter.termNotEqualsFilter(term);
		} else if (similar) {
			return negation ? TextFilter.termNotSimilarFilter(term) : TextFilter.termSimilarFilter(term);
		} else if (startsWith) {
			return negation ? TextFilter.termStartsNotWithFilter(term) : TextFilter.termStartsWithFilter(term);
		} else {
			return negation ? TextFilter.termContainsNotFilter(term) : TextFilter.termContainsFilter(term);
		}
	}

	public Filter createFullTextFilter(TextFilter textFilter, String... fieldNames) {
		return createFullTextFilter(textFilter, true, fieldNames);
	}

	public Filter createFullTextFilter(TextFilter textFilter, boolean orQuery, String... fieldNames) {
		Filter filter = orQuery ? new OrFilter() : new AndFilter();
		if (fieldNames == null || fieldNames.length == 0) {
			columnIndices.stream().filter(columnIndex -> columnIndex.getType() == IndexType.TEXT || columnIndex.getType() == IndexType.TRANSLATABLE_TEXT).forEach(columnIndex -> {
				IndexFilter<TextIndex, TextFilter> indexFilter = new IndexFilter<TextIndex, TextFilter>(columnIndex, textFilter);
				if (orQuery) {
					filter.or(indexFilter);
				} else {
					filter.and(indexFilter);
				}
			});
		} else {
			for (String fieldName : fieldNames) {
				ColumnIndex columnIndex = columnIndexByName.get(fieldName);
				if (columnIndex != null && columnIndex.getType() == IndexType.TEXT || columnIndex.getType() == IndexType.TRANSLATABLE_TEXT) {
					IndexFilter<TextIndex, TextFilter> indexFilter = new IndexFilter<TextIndex, TextFilter>(columnIndex, textFilter);
					if (orQuery) {
						filter.or(indexFilter);
					} else {
						filter.and(indexFilter);
					}
				}
			}
		}
		return filter;
	}

	public List<SortEntry> sortRecords(String columnName, BitSet records, boolean ascending, UserContext userContext, SingleReferenceIndex... path) {
		ColumnIndex column = null;
		if (path != null && path.length > 0) {
			column = path[path.length - 1].getReferencedTable().getColumnIndex(columnName);
		} else {
			column = getColumnIndex(columnName);
		}
		if (column == null) {
			return null;
		}
		List<SortEntry> sortEntries = SortEntry.createSortEntries(records, path);

		return column.sortRecords(sortEntries, ascending, userContext);
	}

	public int createRecord(int recordId) {
		int id = 0;
		if (recordId == 0) {
			if (keepDeletedRecords) {
				id = Math.max(records.getNextId(), deletedRecords.getNextId());
			} else {
				id = records.getNextId();
			}
		} else {
			id = recordId;
			if (keepDeletedRecords && deletedRecords.getValue(recordId)) {
				deletedRecords.setValue(recordId, false);
			}
		}
		records.setValue(id, true);
		return id;
	}

	public void updateFullTextIndex(int id, List<FullTextIndexValue> values, boolean update) {
		if (update) {
			Set<String> textFieldNames = values.stream().map(FullTextIndexValue::getFieldName).collect(Collectors.toSet());
			List<FullTextIndexValue> recordFullTextIndexValues = new ArrayList<>(values);
			for (TextIndex textField : getTextFields()) {
				if (!textFieldNames.contains(textField.getName())) {
					recordFullTextIndexValues.add(new FullTextIndexValue(textField.getName(), textField.getValue(id)));
				}
			}
			for (TranslatableTextIndex translatableTextIndex : getTranslatedTextFields()) {
				if (!textFieldNames.contains(translatableTextIndex.getName())) {
					TranslatableText translatableTextValue = translatableTextIndex.getValue(id);
					if (translatableTextValue != null) {
						recordFullTextIndexValues.add(new FullTextIndexValue(translatableTextIndex.getName(), translatableTextValue));
					}
				}
			}
			collectionTextSearchIndex.setRecordValues(id, recordFullTextIndexValues, true);
		} else {
			collectionTextSearchIndex.setRecordValues(id, values, false);
		}
	}

	private List<Integer> getReferencedRecords(int id, ColumnIndex<?, ?> referenceColumn) {
		if (referenceColumn.getColumnType() == ColumnType.MULTI_REFERENCE) {
			MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) referenceColumn;
			return multiReferenceIndex.getReferencesAsList(id);
		} else {
			SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) referenceColumn;
			int reference = singleReferenceIndex.getValue(id);
			return reference > 0 ? Collections.singletonList(reference) : Collections.emptyList();
		}
	}

	public void deleteRecord(int id) {
		deleteRecord(id, null);
	}

	private void deleteRecord(int id, ColumnIndex<?, ?> cascadeOriginIndex) {
		records.setValue(id, false);
		if (keepDeletedRecords) {
			if (deletedRecords.getValue(id)) {
				return;
			}
			deletedRecords.setValue(id, true);
		}

		for (ColumnIndex<?, ?> referenceColumn : getReferenceColumns()) {
			if (referenceColumn == cascadeOriginIndex) {
				continue;
			}
			ReferenceIndex referenceIndex = (ReferenceIndex) referenceColumn;
			boolean isCascadeDelete = referenceIndex.isCascadeDeleteReferences();
			TableIndex referencedTable = referenceIndex.getReferencedTable();
			boolean isReferenceKeepDeletedRecords = referencedTable.isKeepDeletedRecords();
			boolean isMultiReference = referenceIndex.isMultiReference();
			ColumnIndex<?, ?> backReferenceColumn = referenceColumn.getReferencedColumn();
			boolean isWithBackReferenceColumn = backReferenceColumn != null;
			boolean isMultiBackReference = backReferenceColumn != null && backReferenceColumn.getColumnType() == ColumnType.MULTI_REFERENCE;

			List<Integer> referencedRecords = getReferencedRecords(id, referenceColumn);
			if (referencedRecords.isEmpty()) {
				continue;
			}

			if (keepDeletedRecords) {
				if (isReferenceKeepDeletedRecords) {
					if (isCascadeDelete) {
						//remove no reference!
						//cascade delete
						referencedRecords.forEach(refId -> referencedTable.deleteRecord(refId, backReferenceColumn));
					} else {
						//remove back references
						if (isWithBackReferenceColumn) {
							removeBackReferences(id, backReferenceColumn, isMultiBackReference, referencedRecords);
						}
					}
				} else {
					if (isCascadeDelete) {
						//remove references
						if (isMultiReference) {
							MultiReferenceIndex multiReferenceColumn = (MultiReferenceIndex) referenceColumn;
							multiReferenceColumn.removeAllReferences(id, false);
						} else {
							SingleReferenceIndex singleReferenceColumn = (SingleReferenceIndex) referenceColumn;
							singleReferenceColumn.setValue(id, 0, false);
						}
						//cascade delete
						referencedRecords.forEach(referencedTable::deleteRecord);
					} else {
						//remove back references
						if (isWithBackReferenceColumn) {
							removeBackReferences(id, backReferenceColumn, isMultiBackReference, referencedRecords);
						}
					}
				}
			} else {
				if (isCascadeDelete) {
					referencedRecords.forEach(referencedTable::deleteRecord);
				}
			}
		}

		if (!keepDeletedRecords) {
			for (ColumnIndex<?, ?> columnIndex : columnIndices) {
				columnIndex.removeValue(id);
			}
			if (collectionTextSearchIndex != null) {
				collectionTextSearchIndex.delete(id, getFileFieldNames());
			}
		}
	}

	private void removeBackReferences(int id, ColumnIndex<?, ?> backReferenceColumn, boolean isMultiBackReference, List<Integer> referencedRecords) {
		if (isMultiBackReference) {
			MultiReferenceIndex multiBackReferenceColumn = (MultiReferenceIndex) backReferenceColumn;
			referencedRecords.forEach(refId -> multiBackReferenceColumn.removeReferences(refId, Collections.singletonList(id), true));
		} else {
			SingleReferenceIndex singleBackReferenceColumn = (SingleReferenceIndex) backReferenceColumn;
			referencedRecords.forEach(refId -> {
				int value = singleBackReferenceColumn.getValue(refId);
				if (id == value) {
					singleBackReferenceColumn.setValue(refId, 0, true);
				}
			});
		}
	}

	public void restoreRecord(int id) {
		restoreRecord(id, null);
	}

	public void restoreRecord(int id, ColumnIndex<?, ?> cascadeOriginIndex) {
		if (!keepDeletedRecords || !deletedRecords.getValue(id)) {
			return;
		}
		deletedRecords.setValue(id, false);

		for (ColumnIndex<?, ?> referenceColumn : getReferenceColumns()) {
			if (referenceColumn == cascadeOriginIndex) {
				continue;
			}
			ReferenceIndex referenceIndex = (ReferenceIndex) referenceColumn;
			boolean isCascadeDelete = referenceIndex.isCascadeDeleteReferences();
			TableIndex referencedTable = referenceIndex.getReferencedTable();
			boolean isReferenceKeepDeletedRecords = referencedTable.isKeepDeletedRecords();
			boolean isMultiReference = referenceIndex.isMultiReference();
			ColumnIndex<?, ?> backReferenceColumn = referenceColumn.getReferencedColumn();
			boolean isWithBackReferenceColumn = backReferenceColumn != null;
			boolean isMultiBackReference = backReferenceColumn != null && backReferenceColumn.getColumnType() == ColumnType.MULTI_REFERENCE;

			List<Integer> referencedRecords = getReferencedRecords(id, referenceColumn);
			if (referencedRecords.isEmpty()) {
				continue;
			}

			if (isReferenceKeepDeletedRecords) {
				if (isCascadeDelete) {
					//references are in place
					//cascade restore
					referencedRecords.forEach(refId -> referencedTable.restoreRecord(refId, backReferenceColumn));
				} else {
					//restore back references
					if (isWithBackReferenceColumn) {
						restoreBackReferences(id, referenceColumn, referencedTable, isMultiReference, backReferenceColumn, isMultiBackReference, referencedRecords);
					}
				}
			} else {
				if (isCascadeDelete) {
					//cascade deleted records are gone - nothing to do
				} else {
					if (isWithBackReferenceColumn) {
						//restore back references
						restoreBackReferences(id, referenceColumn, referencedTable, isMultiReference, backReferenceColumn, isMultiBackReference, referencedRecords);
					}
				}
			}
		}

		records.setValue(id, true);
	}

	private void restoreBackReferences(int id, ColumnIndex<?, ?> referenceColumn, TableIndex referencedTable, boolean isMultiReference, ColumnIndex<?, ?> backReferenceColumn, boolean isMultiBackReference, List<Integer> referencedRecords) {
		if (isMultiBackReference) {
			MultiReferenceIndex multiBackReferenceColumn = (MultiReferenceIndex) backReferenceColumn;
			referencedRecords.forEach(refId -> {
				if (referencedTable.isStored(refId)) {
					multiBackReferenceColumn.addReferences(refId, Collections.singletonList(id), true);
				} else {
					//remove local refs
					if (isMultiReference) {
						MultiReferenceIndex multiReferenceColumn = (MultiReferenceIndex) referenceColumn;
						multiReferenceColumn.removeAllReferences(id, true);
					} else {
						SingleReferenceIndex singleReferenceColumn = (SingleReferenceIndex) referenceColumn;
						singleReferenceColumn.setValue(id, 0,true);
					}
				}
			});
		} else {
			SingleReferenceIndex singleBackReferenceColumn = (SingleReferenceIndex) backReferenceColumn;
			referencedRecords.forEach(refId -> {
				if (referencedTable.isStored(refId)) {
					int value = singleBackReferenceColumn.getValue(refId);
					if (value == 0) {
						singleBackReferenceColumn.setValue(refId, id, true);
					} else {
						//remove local refs
						if (isMultiReference) {
							MultiReferenceIndex multiReferenceColumn = (MultiReferenceIndex) referenceColumn;
							multiReferenceColumn.removeAllReferences(id, true);
						} else {
							SingleReferenceIndex singleReferenceColumn = (SingleReferenceIndex) referenceColumn;
							singleReferenceColumn.setValue(id, 0,true);
						}
					}
				}
			});
		}
	}


	public void setTransactionId(int id, long transactionId) {
		if (!tableConfig.isCheckpoints()) {
			return;
		}
		if (transactionIndex == null) {
			transactionIndex = getTransactionIndex();
		}
		transactionIndex.setValue(id, transactionId);
	}

	public long getTransactionId(int id) {
		if (id == 0 || !tableConfig.isCheckpoints()) {
			return 0;
		}
		if (transactionIndex == null) {
			transactionIndex = getTransactionIndex();
		}
		return transactionIndex.getValue(id);
	}

	private LongIndex getTransactionIndex() {
		if (!tableConfig.isCheckpoints()) {
			return null;
		}
		return (LongIndex) getColumnIndex(Table.FIELD_CHECKPOINTS);
	}

	private List<String> getFileFieldNames() {
		if (fileFieldNames == null) {
			fileFieldNames = columnIndices.stream()
					.filter(index -> index.getType() == IndexType.FILE)
					.map(ColumnIndex::getName)
					.collect(Collectors.toList());
		}
		return fileFieldNames;
	}

	private List<TextIndex> getTextFields() {
		if (textFields == null) {
			textFields = columnIndices.stream()
					.filter(index -> index.getType() == IndexType.TEXT)
					.map(index -> (TextIndex) index)
					.collect(Collectors.toList());
		}
		return textFields;
	}

	private List<TranslatableTextIndex> getTranslatedTextFields() {
		if (translatedTextFields == null) {
			translatedTextFields = columnIndices.stream()
					.filter(index -> index.getType() == IndexType.TRANSLATABLE_TEXT)
					.map(index -> (TranslatableTextIndex) index)
					.collect(Collectors.toList());
		}
		return translatedTextFields;
	}

	public BitSet getRecordBitSet() {
		return records.getBitSet();
	}

	public BitSet getDeletedRecordsBitSet() {
		if (!keepDeletedRecords) {
			return null;
		}
		return deletedRecords.getBitSet();
	}

	public List<ColumnIndex> getColumnIndices() {
		return columnIndices;
	}

	public List<ColumnIndex> getReferenceColumns() {
		return columnIndices.stream()
				.filter(column -> column.getColumnType().isReference())
				.collect(Collectors.toList());
	}

	public ColumnIndex getColumnIndex(String name) {
		return columnIndexByName.get(name);
	}

	public boolean isKeepDeletedRecords() {
		return keepDeletedRecords;
	}

	public int getMappingId() {
		return mappingId;
	}

	@Override
	public void setMappingId(int id) {
		if (mappingId > 0 && mappingId != id) {
			throw new RuntimeException("Error mapping index with different id:" + mappingId + " -> " + id);
		}
		if (mappingId > 0) {
			return;
		}
		this.mappingId = id;
		this.indexMetaData.setMappingId(id);
	}

	public void merge(Table table) {
		if (!tableConfig.keepDeleted() && table.getTableConfig().keepDeleted()) {
			keepDeletedRecords = true;
			deletedRecords = new BooleanIndex("coll-del-recs", this, ColumnType.BOOLEAN);
			logger.warn("Updated table {} with keep deleted option", getFQN());
		}
		this.tableConfig.merge(table.getTableConfig());
		for (Column column : table.getColumns()) {
			ColumnIndex localColumn = getColumnIndex(column.getName());
			if (localColumn == null) {
				localColumn = ColumnIndex.createColumn(this, column.getName(), column.getType());
				addIndex(localColumn);
			}
			if (localColumn.getMappingId() == 0) {
				localColumn.setMappingId(column.getMappingId());
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("collection: ").append(name).append(", id:").append(mappingId).append("\n");
		for (ColumnIndex column : columnIndices) {
			sb.append("\t").append(column.toString()).append("\n");
		}
		return sb.toString();
	}

	public void close() {
		try {
			logger.info("Shutdown on collection:" + name);
			if (collectionTextSearchIndex != null) {
				collectionTextSearchIndex.commit(true);
			}
			records.setValue(0, true);
			records.close();
			for (ColumnIndex column : columnIndices) {
				column.close();
			}
			recordVersioningIndex.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void drop() {
		collectionTextSearchIndex.drop();
		for (ColumnIndex column : columnIndices) {
			column.drop();
		}
	}

	public String getFQN() {
		return parentFQN + "." + name;
	}

	public String getName() {
		return name;
	}

	public DatabaseIndex getDatabaseIndex() {
		return databaseIndex;
	}
}
