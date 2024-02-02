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
import org.teamapps.message.protocol.message.AttributeType;
import org.teamapps.message.protocol.message.MessageDefinition;
import org.teamapps.message.protocol.message.MessageModelCollection;
import org.teamapps.message.protocol.model.EnumDefinition;
import org.teamapps.message.protocol.model.ModelCollection;
import org.teamapps.message.protocol.model.ModelCollectionProvider;
import org.teamapps.message.protocol.service.ServiceProtocol;

public class MessageProtocol implements ModelCollectionProvider {
	@Override
	public ModelCollection getModelCollection() {
		MessageModelCollection modelCollection = new MessageModelCollection("newTestModel", "org.teamapps.udb.model", 1);

		MessageDefinition fileContentData = modelCollection.createModel("fileContentData", "#udb.fileContentData");

		fileContentData.addString("name", 1);
		fileContentData.addLong("fileSize", 2);
		fileContentData.addString("hash", 3);
		fileContentData.addString("content", 4);
		fileContentData.addString("mimeType", 5);
		fileContentData.addString("createdBy", 6);
		fileContentData.addString("modifiedBy", 7);
		fileContentData.addTimestamp("dateCreated", 8);
		fileContentData.addTimestamp("dateModified", 9);
		fileContentData.addInteger("pages", 10);
		fileContentData.addString("title", 11);
		fileContentData.addString("latitude", 12);
		fileContentData.addString("longitude", 13);
		fileContentData.addString("device", 14);
		fileContentData.addString("software", 15);
		fileContentData.addString("softwareVersion", 16);
		fileContentData.addString("duration", 17);
		fileContentData.addInteger("imageWidth", 18);
		fileContentData.addInteger("imageHeight", 19);
		fileContentData.addString("language", 20);
		fileContentData.addStringArray("metaKeys", 21);
		fileContentData.addStringArray("metaValues", 22);


		return modelCollection;

	}
}
