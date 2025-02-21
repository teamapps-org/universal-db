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
package org.teamapps.universaldb.model;

import org.teamapps.message.protocol.utils.MessageUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FieldModel implements BaseFieldModel {
	private final static int FIELD_MODEL_VERSION = 1;

	private final String name;
	private String title;
	private final TableModel tableModel;
	private final FieldType fieldType;
	private int fieldId;
	private boolean deprecated;
	private boolean deleted;
	private int dateCreated;
	private int dateModified;
	private int versionCreated;
	private int versionModified;

	protected FieldModel(String title, TableModel tableModel, FieldType fieldType) {
		this(title, title, tableModel, fieldType);
	}

	protected FieldModel(String name, String title, TableModel tableModel, FieldType fieldType) {
		NamingUtils.checkName(name, title);
		this.name = NamingUtils.createName(name);
		this.title = NamingUtils.createTitle(title);
		this.tableModel = tableModel;
		this.fieldType = fieldType;
	}

	protected FieldModel(DataInputStream dis, TableModel model) throws IOException {
		int modelVersion = dis.readInt();
		name = MessageUtils.readString(dis);
		title = MessageUtils.readString(dis);
		tableModel = model;
		fieldType = FieldType.getTypeById(dis.readInt());
		fieldId = dis.readInt();
		deprecated = dis.readBoolean();
		deleted = dis.readBoolean();
		dateCreated = dis.readInt();
		dateModified = dis.readInt();
		versionCreated = dis.readInt();
		versionModified = dis.readInt();
	}

	public void write(DataOutputStream dos) throws IOException {
		dos.writeInt(FIELD_MODEL_VERSION);
		MessageUtils.writeString(dos, name);
		MessageUtils.writeString(dos, title);
		dos.writeInt(fieldType.getId());
		dos.writeInt(fieldId);
		dos.writeBoolean(deprecated);
		dos.writeBoolean(deleted);
		dos.writeInt(dateCreated);
		dos.writeInt(dateModified);
		dos.writeInt(versionCreated);
		dos.writeInt(versionModified);
	}

	public String getName() {
		return name;
	}

	public String getTitle() {
		return title;
	}

	protected void setTitle(String title) {
		this.title = title;
	}

	public TableModel getTableModel() {
		return tableModel;
	}

	public FieldType getFieldType() {
		return fieldType;
	}

	public int getFieldId() {
		return fieldId;
	}

	protected void setFieldId(int fieldId) {
		if (this.fieldId != 0) {
			throw new RuntimeException("Error: field id already set:" + this.fieldId + ", new:" + fieldId);
		}
		this.fieldId = fieldId;
	}

	public boolean isDeprecated() {
		return deprecated;
	}

	protected void setDeprecated(boolean deprecated) {
		this.deprecated = deprecated;
	}

	public boolean isDeleted() {
		return deleted;
	}

	protected void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public int getDateCreated() {
		return dateCreated;
	}

	protected void setDateCreated(int dateCreated) {
		this.dateCreated = dateCreated;
	}

	public int getDateModified() {
		return dateModified;
	}

	protected void setDateModified(int dateModified) {
		this.dateModified = dateModified;
	}

	public int getVersionCreated() {
		return versionCreated;
	}

	protected void setVersionCreated(int versionCreated) {
		this.versionCreated = versionCreated;
	}

	public int getVersionModified() {
		return versionModified;
	}

	protected void setVersionModified(int versionModified) {
		this.versionModified = versionModified;
	}

	public boolean isMetaField() {
		return TableModel.isReservedMetaName(name);
	}

	@Override
	public String toString() {
		return name + " (" + title + "): " + fieldType.name() + " [" + fieldId + "]";
	}
}
