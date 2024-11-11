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
