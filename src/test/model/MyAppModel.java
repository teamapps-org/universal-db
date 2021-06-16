import org.teamapps.universaldb.schema.*;

public class MyAppModel implements SchemaInfoProvider {

    public Schema getSchema() {
        Schema schema = Schema.create("org.teamapps.myapp.model");
        schema.setSchemaName("MyAppSchema");
        Database database = schema.addDatabase("myAppDb");
        Table user = database.addTable("user");
        Table project = database.addTable("project");
        user
                .addText("name")
                .addText("text2")
                .addTimestamp("timestamp")
                .addEnum("enumValue", "value1", "value2", "value3")
                .addInteger("number")
                .addLong("bigNumber")
                .addReference("projects", project, true, "manager");

        project
                .addText("title")
                .addReference("manager", user, false, "projects");

        return schema;
    }
}