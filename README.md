
# UniversalDB

An ultra fast embedded persisting in-memory database:

* Easy and intuitive API through generated POJOs
* Integrated full text search - including complex joins
* Integrated file storage - either local or S3 compatible storage with AWS or min.io
* File parsing and full text indexing

### Setting up the dependencies

Current UniversalDB version: [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.teamapps/universal-db/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.teamapps/universal-db)
```xml
<dependency>
  <groupId>org.teamapps</groupId>
  <artifactId>universal-db</artifactId>
  <version>0.5.2</version>
</dependency>
```

## Build configuration

Add this to your pom.xml:

```xml
    <build>
        <plugins>
            <plugin>
                <groupId>org.teamapps</groupId>
                <artifactId>universal-db-maven-plugin</artifactId>
                <version>1.2</version>
                <executions>
                    <execution>
                        <phase>generate-sources</phase>
                        <goals>
                            <goal>generate-model</goal>
                        </goals>
                        <configuration>
                            <modelSourceDirectory>${project.basedir}/src/main/model</modelSourceDirectory>
                            <modelClasses>
                                <modelClass>MyAppModel</modelClass>
                            </modelClasses>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```

## Create UniversalDB schema 

Add a source folder e.g. `/src/main/model` and create a model class e.g. `MyAppModel` with the model you want to use:

```java
import org.teamapps.universaldb.schema.*;

public class MyAppModel implements ModelProvider {

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
```
After compiling you will have a User and a Project class to work with.

```java
package org.teamapps.myapp;

import org.teamapps.myapp.model.myappdb.Project;
import org.teamapps.myapp.model.myappdb.User;
import org.teamapps.myapp.model.myappdb.UserQuery;
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.myapp.model.MyAppSchema;
import org.teamapps.universaldb.index.text.TextFilter;

import java.io.File;
import java.util.List;

public class MyApp {

    public static void main(String[] args) throws Exception {
        startDb();
        createInitialData();
        query1();
        filterQuery1();
    }
    private static void startDb() throws Exception {
        File storagePath = new File("./server-data/db-storage");
        if (!storagePath.exists()) {
            if (!storagePath.mkdirs()) System.out.println("Error creating Database directory!");
        }
        UniversalDB.createStandalone(storagePath, new MyAppSchema());
    }
    private static void createInitialData() {
        if (User.getCount() > 0) {
            System.out.println("DB already contains data, don't create new entries");
            return;
        }
        User user1 = User.create().setName("FirstUser").save();
        Project project1 = Project.create().setTitle("ProjectOne");
        user1.addProjects(project1).save();
    }
    private static void query1() {
        User.getAll().forEach(user -> {
            user.getProjects().forEach(project -> {
                System.out.println(user.getName()+ " has project " + project.getTitle());
            });
        });
    }
    private static void filterQuery1() {
        UserQuery firstQuery = User.filter().name(TextFilter.termContainsFilter("First"));
        List<User> firstUserList = firstQuery.execute();
        long firstUserProjectCount = Project.filter().filterManager(firstQuery).execute().stream().count();
        System.out.println("There are " + firstUserProjectCount + " projects where the Manager's name contains 'First'");
    }
}

```

## License

The UniversalDB maven plugin is released under version 2.0 of the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).
