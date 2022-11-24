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
import org.teamapps.protocol.schema.MessageModelCollection;
import org.teamapps.protocol.schema.ModelCollection;
import org.teamapps.protocol.schema.ModelCollectionProvider;
import org.teamapps.protocol.schema.ObjectPropertyDefinition;

public class Protocol implements ModelCollectionProvider {

	@Override
	public ModelCollection getModelCollection() {
		MessageModelCollection modelCollection = new MessageModelCollection("udbTestProtocol", "org.teamapps.test.protocol", 1);

		ObjectPropertyDefinition message = modelCollection.createModel("testMessage", "testMessage");
		ObjectPropertyDefinition user = modelCollection.createModel("testUser", "testUser");
		ObjectPropertyDefinition attachment = modelCollection.createModel("testAttachment", "testAttachment");

		message.addIntProperty("messageId", 1);
		message.addLongProperty("date", 2);
		message.addStringProperty("subject", 3);
		message.addStringProperty("body", 4);
		message.addSingleReference("author", 5, user);
		message.addMultiReference("recipients", 6, user);
		message.addMultiReference("attachments", 7, attachment);
		message.addIntProperty("testId", 8);

		user.addIntProperty("userId", 1);
		user.addStringProperty("firstName", 2);
		user.addStringProperty("lastName", 3);
		user.addByteArrayProperty("avatar", 4);
		user.addIntProperty("testId", 5);

		attachment.addStringProperty("fileName", 1);
		attachment.addLongProperty("fileSize", 2);
		attachment.addFileProperty("file", 3);
		attachment.addIntProperty("testId", 4);

		return modelCollection;
	}
}
