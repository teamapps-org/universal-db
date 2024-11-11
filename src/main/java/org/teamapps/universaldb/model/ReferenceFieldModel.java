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
import java.util.List;
import java.util.function.Function;

public class ReferenceFieldModel extends FieldModel {

	private TableModel referencedTable;
	private final boolean multiReference;
	private final boolean cascadeDelete;
	private ReferenceFieldModel reverseReferenceField;

	protected ReferenceFieldModel(String title, TableModel tableModel, TableModel referencedTable, boolean multiReference, boolean cascadeDelete, ReferenceFieldModel reverseReferenceField) {
		this(title, title, tableModel, referencedTable, multiReference, cascadeDelete, reverseReferenceField);
	}

	protected ReferenceFieldModel(String name, String title, TableModel tableModel, TableModel referencedTable, boolean multiReference, boolean cascadeDelete, ReferenceFieldModel reverseReferenceField) {
		super(name, title, tableModel, multiReference ? FieldType.MULTI_REFERENCE : FieldType.SINGLE_REFERENCE);
		this.referencedTable = referencedTable;
		this.multiReference = multiReference;
		this.cascadeDelete = cascadeDelete;
		setReverseReferenceField(reverseReferenceField);
	}

	protected ReferenceFieldModel(DataInputStream dis, TableModel tableModel, List<Function<DatabaseModel, Boolean>> resolveFunctions) throws IOException {
		super(dis, tableModel);
		String referencedTableName = MessageUtils.readString(dis);
		resolveFunctions.add(databaseModel -> {
			TableModel table = databaseModel.getTable(referencedTableName);
			setReferencedTable(table);
			return table != null;
		});
		this.multiReference = dis.readBoolean();
		this.cascadeDelete = dis.readBoolean();
		if (dis.readBoolean()) {
			String reverseReferenceTableName = MessageUtils.readString(dis);
			String reverseReferenceFieldName = MessageUtils.readString(dis);
			resolveFunctions.add(databaseModel -> {
				ReferenceFieldModel referenceField = databaseModel.getReferenceField(reverseReferenceTableName, reverseReferenceFieldName);
				setReverseReferenceField(referenceField);
				return referenceField != null;
			});
		}
	}

	public void write(DataOutputStream dos) throws IOException {
		super.write(dos);
		MessageUtils.writeString(dos, referencedTable.getName());
		dos.writeBoolean(multiReference);
		dos.writeBoolean(cascadeDelete);
		if (reverseReferenceField == null) {
			dos.writeBoolean(false);
		} else {
			dos.writeBoolean(true);
			MessageUtils.writeString(dos, reverseReferenceField.getTableModel().getName());
			MessageUtils.writeString(dos, reverseReferenceField.getName());
		}
	}

	public TableModel getReferencedTable() {
		return referencedTable;
	}

	private void setReferencedTable(TableModel referencedTable) {
		this.referencedTable = referencedTable;
	}

	public boolean isMultiReference() {
		return multiReference;
	}

	public boolean isCascadeDelete() {
		return cascadeDelete;
	}

	public ReferenceFieldModel getReverseReferenceField() {
		return reverseReferenceField;
	}

	protected void setReverseReferenceField(ReferenceFieldModel reverseReferenceField) {
		if (reverseReferenceField == null) {
			return;
		}
		this.reverseReferenceField = reverseReferenceField;
		if (reverseReferenceField.getReverseReferenceField() == null) {
			reverseReferenceField.setReverseReferenceField(this);
		}
	}
}
