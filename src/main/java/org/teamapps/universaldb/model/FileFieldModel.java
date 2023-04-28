package org.teamapps.universaldb.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileFieldModel extends FieldModel {

	private final boolean indexContent;
	private final int maxIndexContentLength;
	private final boolean detectLanguage;


	protected FileFieldModel(String title, TableModel tableModel) {
		this(title, title, tableModel, true, 100_000, true);
	}

	protected FileFieldModel(String name, String title, TableModel tableModel, boolean indexContent, int maxIndexContentLength, boolean detectLanguage) {
		super(name, title, tableModel, FieldType.FILE);
		this.indexContent = indexContent;
		this.maxIndexContentLength = maxIndexContentLength;
		this.detectLanguage = detectLanguage;
	}

	protected FileFieldModel(DataInputStream dis, TableModel model) throws IOException {
		super(dis, model);
		indexContent = dis.readBoolean();
		maxIndexContentLength = dis.readInt();
		detectLanguage = dis.readBoolean();
	}

	public void write(DataOutputStream dos) throws IOException {
		super.write(dos);
		dos.writeBoolean(indexContent);
		dos.writeInt(maxIndexContentLength);
		dos.writeBoolean(detectLanguage);
	}

	public boolean isIndexContent() {
		return indexContent;
	}

	public int getMaxIndexContentLength() {
		return maxIndexContentLength;
	}

	public boolean isDetectLanguage() {
		return detectLanguage;
	}
}
