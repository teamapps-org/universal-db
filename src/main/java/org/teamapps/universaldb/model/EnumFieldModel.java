package org.teamapps.universaldb.model;

import org.teamapps.message.protocol.utils.MessageUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class EnumFieldModel extends FieldModel {

	private EnumModel enumModel;

	protected EnumFieldModel(String title, TableModel tableModel, EnumModel enumModel) {
		this(title, title, tableModel, enumModel);
	}

	protected EnumFieldModel(String name, String title, TableModel tableModel, EnumModel enumModel) {
		super(name, title, tableModel, FieldType.ENUM);
		this.enumModel = enumModel;
	}

	protected EnumFieldModel(DataInputStream dis, TableModel model, DatabaseModel databaseModel) throws IOException {
		super(dis, model);
		String enumModelName = MessageUtils.readString(dis);
		this.enumModel = databaseModel.getEnumModel(enumModelName);
	}

	public void write(DataOutputStream dos) throws IOException {
		super.write(dos);
		MessageUtils.writeString(dos, enumModel.getName());
	}

	public EnumModel getEnumModel() {
		return enumModel;
	}

	protected void setEnumModel(EnumModel enumModel) {
		this.enumModel = enumModel;
	}
}
