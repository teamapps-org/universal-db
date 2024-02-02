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
		MessageModelCollection modelCollection = new MessageModelCollection("newTestModel", "org.teamapps.test.protocol", 1);

		MessageDefinition employee = modelCollection.createModel("employee", "col.employee", true);
		MessageDefinition company = modelCollection.createModel("company", "col.company", true);
		MessageDefinition person1 = modelCollection.createModel("person1", "col.person", true);

		EnumDefinition employeeType = modelCollection.createEnum("employeeType", "fullTime", "partTime", "seasonal", "temporary");
		EnumDefinition gender = modelCollection.createEnum("gender", "male", "female", "diverse");


		employee.addAttribute("firstName", 1, AttributeType.STRING);
		employee.addAttribute("lastName", 2, AttributeType.STRING);
		employee.addAttribute("pic", 3, AttributeType.BYTE_ARRAY);
		employee.addAttribute("vegan", 4, AttributeType.BOOLEAN);
		employee.addAttribute("birthday", 5, AttributeType.DATE);
		employee.addEnum("type", employeeType, 6);
		employee.addEnum("gender", gender, 7);
		employee.addSingleReference("mentor", 8, employee);

		company.addAttribute("name", 1, AttributeType.STRING);
		company.addAttribute("type", 2, AttributeType.STRING);
		company.addSingleReference("ceo", 3, employee);
		company.addMultiReference("employee", 4, employee);
		company.addAttribute("picture", 5, AttributeType.FILE);

		person1.addString("name", 1);
		person1.addString("email", 2);

		ServiceProtocol testService = modelCollection.createService("testService");
		testService.addMethod("method1", company, employee);

		MessageDefinition message = modelCollection.createModel("testMessage", "testMessage", true);
		MessageDefinition user = modelCollection.createModel("testUser", "testUser", true);
		MessageDefinition attachment = modelCollection.createModel("testAttachment", "testAttachment", false);

		message.addInteger("messageId", 1);
		message.addLong("date", 2);
		message.addString("subject", 3);
		message.addString("body", 4);
		message.addSingleReference("author", 5, user);
		message.addMultiReference("recipients", 6, user);
		message.addMultiReference("attachments", 7, attachment);
		message.addInteger("testId", 8);

		user.addInteger("userId", 1);
		user.addString("firstName", 2);
		user.addString("lastName", 3);
		user.addByteArray("avatar", 4);
		user.addInteger("testId", 5);

		attachment.addString("fileName", 1);
		attachment.addLong("fileSize", 2);
		attachment.addFile("file", 3);
		attachment.addInteger("testId", 4);

		return modelCollection;

	}
}
