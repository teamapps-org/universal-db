import org.teamapps.protocol.message.MessageDefinition;
import org.teamapps.protocol.message.MessageModelCollection;
import org.teamapps.protocol.model.ModelCollection;
import org.teamapps.protocol.model.ModelCollectionProvider;

public class MessageProtocol implements ModelCollectionProvider {
	@Override
	public ModelCollection getModelCollection() {
		MessageModelCollection modelCollection = new MessageModelCollection("udbTestProtocol", "org.teamapps.test.protocol", 1);

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
