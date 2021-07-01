/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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
