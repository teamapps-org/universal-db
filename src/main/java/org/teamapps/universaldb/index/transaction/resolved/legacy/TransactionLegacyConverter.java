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
package org.teamapps.universaldb.index.transaction.resolved.legacy;

import org.teamapps.universaldb.index.ColumnType;
import org.teamapps.universaldb.index.MappedObject;
import org.teamapps.universaldb.index.log.DefaultLogIndex;
import org.teamapps.universaldb.index.log.LogIterator;
import org.teamapps.universaldb.index.log.RotatingLogIndex;
import org.teamapps.universaldb.schema.Column;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.schema.Table;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransactionLegacyConverter {


	public static void convertLegacyTransactionIndex(File transactionsPath, File fileStorePath, Set<String> filteredDbs) throws Exception {
		RotatingLogIndex logIndex = new RotatingLogIndex(transactionsPath, "transactions");
		DefaultLogIndex models = new DefaultLogIndex(transactionsPath, "schemas");
		Schema schema = models.readAllLogs()
				.stream()
				.map(bytes -> {
					try {
						DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
						Schema model = new Schema(dis);
						dis.readLong();
						dis.readLong();
						return model;
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				})
				.reduce((first, second) -> second).orElse(null);
		Map<Integer, String> fileFieldById = schema.getDatabases().stream()
				.flatMap(db -> db.getTables().stream())
				.flatMap(table -> table.getColumns().stream())
				.filter(column -> column.getType() == ColumnType.FILE)
				.collect(Collectors.toMap(Column::getMappingId, column -> column.getFQN().replace('.', '/')));

		Map<Integer, String> fqnById = Stream.concat(Stream.concat(
						schema.getDatabases().stream().map(e -> (MappedObject) e),
						schema.getDatabases().stream().flatMap(db -> db.getTables().stream()).map(e -> (MappedObject) e)
				),
				schema.getDatabases().stream().flatMap(db -> db.getTables().stream()).flatMap(table -> table.getColumns().stream()).map(e -> (MappedObject) e)
		).collect(Collectors.toMap(MappedObject::getMappingId, MappedObject::getFQN));


		Map<Integer, String> dbByMappingId = new HashMap<>();
		schema.getDatabases().stream().forEach(e -> dbByMappingId.put(e.getMappingId(), e.getFQN()));
		schema.getDatabases().stream().flatMap(db -> db.getTables().stream()).forEach(e -> dbByMappingId.put(e.getMappingId(), e.getDatabase().getFQN()));
		schema.getDatabases().stream().flatMap(db -> db.getTables().stream()).flatMap(t -> t.getColumns().stream()).forEach(e -> dbByMappingId.put(e.getMappingId(), e.getTable().getDatabase().getFQN()));

		BiFunction<String, Integer, File> fileByUuidAndFieldId = (uuid, fieldId) -> {
			String relativePath = fileFieldById.get(fieldId);
			return new File(fileStorePath, relativePath + "/" + uuid.substring(0, 4) + "/" + uuid);
		};

		LegacyResolvedTransactionRecordValue.setFileByUuidAndFieldId(fileByUuidAndFieldId);

		Map<String, Integer> recordsByTable = new HashMap<>();
		int maxRecords = 0;

		LogIterator iterator = logIndex.readLogs();
		int counter = 0;
		int schemaUpdates = 0;
		int countManyRecs = 0;
		int countMoreThanOne = 0;
		long time = System.currentTimeMillis();
		while (iterator.hasNext()) {
			byte[] bytes = iterator.next();
			LegacyResolvedTransaction resolvedTransaction = LegacyResolvedTransaction.createResolvedTransaction(bytes);

			int size = resolvedTransaction.getTransactionRecords() == null ? 0 : resolvedTransaction.getTransactionRecords().size();
			if (size > 0) {
				recordsByTable.compute(dbByMappingId.get(resolvedTransaction.getTransactionRecords().get(0).getTableId()), (k, v) -> v == null ? 1 : v + 1);

				if (size > 1) {
					long count = resolvedTransaction.getTransactionRecords().stream().map(tr -> dbByMappingId.get(tr.getTableId())).distinct().count();
					if (count > 1) {
						System.out.println("Multi dbs:" + count);
					}
					countMoreThanOne++;
				}
			} else {
				schemaUpdates++;
			}
			if (size > 100) {
				countManyRecs++;
			}
			maxRecords = Math.max(maxRecords, size);
			counter++;
			if (counter % 250_000 == 0) {
				System.out.println("Transactions: " + counter + ", files:" + LegacyResolvedTransactionRecordValue.fileCounter + ", time: " + (System.currentTimeMillis() - time));
			}
		}
		iterator.close();
		LegacyResolvedTransactionRecordValue.pathCounts.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));
		System.out.println("Schema updates:" + schemaUpdates);
		System.out.println("Max records: " + maxRecords);
		System.out.println("Many records: " + countManyRecs);
		System.out.println("More than one: " + countMoreThanOne);
		System.out.println("---");
		recordsByTable.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));
		System.out.println("---");
		System.out.println("DONE, time: " + (System.currentTimeMillis() - time) + ", files:" + LegacyResolvedTransactionRecordValue.fileCounter);
	}

}
