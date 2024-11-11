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

import org.teamapps.message.protocol.utils.MessageUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ViewModel {

	private final String name;
	private final String title;
	private TableModel table;
	private final List<FieldModel> fields = new ArrayList<>();
	private boolean deprecated;
	private boolean deleted;
	private int dateCreated;
	private int dateModified;
	private int versionCreated;
	private int versionModified;

	protected ViewModel(String title, TableModel table) {
		this.name = NamingUtils.createName(title);
		this.title = NamingUtils.createTitle(title);
		this.table = table;
	}

	protected ViewModel(DataInputStream dis, List<Function<DatabaseModel, Boolean>> resolveFunctions) throws IOException {
		name = MessageUtils.readString(dis);
		title = MessageUtils.readString(dis);
		String tableName = MessageUtils.readString(dis);
		resolveFunctions.add(databaseModel -> {
			TableModel table = databaseModel.getTable(tableName);
			setTable(table);
			return table != null;
		});
		table = null;
		deprecated = dis.readBoolean();
		deleted = dis.readBoolean();
		dateCreated = dis.readInt();
		dateModified = dis.readInt();
		versionCreated = dis.readInt();
		versionModified = dis.readInt();
		int fieldCount = dis.readInt();
		for (int i = 0; i < fieldCount; i++) {
			String fieldName = MessageUtils.readString(dis);
			resolveFunctions.add(databaseModel -> {
				FieldModel field = databaseModel.getField(tableName, fieldName);
				addField(field);
				return field != null;
			});
		}
	}

	public void write(DataOutputStream dos) throws IOException {
		MessageUtils.writeString(dos, name);
		MessageUtils.writeString(dos, title);
		MessageUtils.writeString(dos, table.getName());
		dos.writeBoolean(deprecated);
		dos.writeBoolean(deleted);
		dos.writeInt(dateCreated);
		dos.writeInt(dateModified);
		dos.writeInt(versionCreated);
		dos.writeInt(versionModified);
		dos.writeInt(fields.size());
		for (FieldModel field : fields) {
			MessageUtils.writeString(dos, field.getName());
		}
	}

	public void setTable(TableModel table) {
		this.table = table;
	}

	public ViewModel addField(FieldModel field) {
		fields.add(field);
		return this;
	}

	public String getName() {
		return name;
	}

	public String getTitle() {
		return title;
	}

	public TableModel getTable() {
		return table;
	}

	public List<FieldModel> getFields() {
		return fields;
	}

	public boolean isDeprecated() {
		return deprecated;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public int getDateCreated() {
		return dateCreated;
	}

	public int getDateModified() {
		return dateModified;
	}

	public int getVersionCreated() {
		return versionCreated;
	}

	public int getVersionModified() {
		return versionModified;
	}

	protected void setDeprecated(boolean deprecated) {
		this.deprecated = deprecated;
	}

	protected void setDateCreated(int dateCreated) {
		this.dateCreated = dateCreated;
	}

	protected void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	protected void setDateModified(int dateModified) {
		this.dateModified = dateModified;
	}

	protected void setVersionCreated(int versionCreated) {
		this.versionCreated = versionCreated;
	}

	protected void setVersionModified(int versionModified) {
		this.versionModified = versionModified;
	}
}
