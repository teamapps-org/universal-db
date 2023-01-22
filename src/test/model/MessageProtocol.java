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
		MessageModelCollection modelCollection = new MessageModelCollection("newTestModel", "org.teamapps.protocol.test", 1);

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

		return modelCollection;
	}
}
