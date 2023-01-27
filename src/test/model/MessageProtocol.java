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

		person1.addStringAttribute("name", 1);
		person1.addStringAttribute("email", 2);

		ServiceProtocol testService = modelCollection.createService("testService");
		testService.addMethod("method1", company, employee);

		MessageDefinition message = modelCollection.createModel("testMessage", "testMessage", true);
		MessageDefinition user = modelCollection.createModel("testUser", "testUser", true);
		MessageDefinition attachment = modelCollection.createModel("testAttachment", "testAttachment", false);

		message.addIntAttribute("messageId", 1);
		message.addLongAttribute("date", 2);
		message.addStringAttribute("subject", 3);
		message.addStringAttribute("body", 4);
		message.addSingleReference("author", 5, user);
		message.addMultiReference("recipients", 6, user);
		message.addMultiReference("attachments", 7, attachment);
		message.addIntAttribute("testId", 8);

		user.addIntAttribute("userId", 1);
		user.addStringAttribute("firstName", 2);
		user.addStringAttribute("lastName", 3);
		user.addByteArrayAttribute("avatar", 4);
		user.addIntAttribute("testId", 5);

		attachment.addStringAttribute("fileName", 1);
		attachment.addLongAttribute("fileSize", 2);
		attachment.addFileAttribute("file", 3);
		attachment.addIntAttribute("testId", 4);

		return modelCollection;

	}
}
