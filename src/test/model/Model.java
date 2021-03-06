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

public class Model implements SchemaInfoProvider {

	public Schema getSchema() {
		Schema schema = Schema.create();
		schema.setSchemaName("TestBaseSchema");
		Database database = schema.addDatabase("testDb1");
		Table table = database.addTable("fieldTest", TableOption.CHECKPOINTS, TableOption.HIERARCHY, TableOption.TRACK_CREATION, TableOption.TRACK_MODIFICATION, TableOption.KEEP_DELETED);
		table
				.addBoolean("booleanField")
				.addShort("shortField")
				.addInteger("intField")
				.addLong("longField")
				.addFloat("floatField")
				.addDouble("doubleField")
				.addText("textField")
				.addTranslatableText("translatableText")
				.addTimestamp("timestampField")
				.addTime("timeField")
				.addDate("dateField")
				.addDateTime("dateTimeField")
				.addLocalDate("localDateField")
				.addEnum("enumField", "firstValue", "secondValue", "thirdValue", "forthValue", "fifthValue")
				.addFile("fileField")
				.addBinary("binaryField")
				.addReference("singleReferenceField", table, false, "backRefSingleReferenceField")
				.addReference("backRefSingleReferenceField", table, false, "singleReferenceField")
				.addReference("singleReferenceNoBackRefField", table, false)
				.addReference("multiReferenceField", table, true, "backRefMultiReferenceField")
				.addReference("backRefMultiReferenceField", table, true, "multiReferenceField")
				.addReference("parent", table, false, "children")
				.addReference("children", table, true, "parent");

		Table fieldTestView = database.addView("fieldTestView", table);

		fieldTestView
				.addBoolean("booleanField")
				.addShort("shortField")
				.addInteger("intField")
				.addLong("longField")
				.addFloat("floatField")
				.addDouble("doubleField")
				.addText("textField")
				.addTranslatableText("translatableText")
				.addTimestamp("timestampField")
				.addTime("timeField")
				.addDate("dateField")
				.addDateTime("dateTimeField")
				.addLocalDate("localDateField")
				.addEnum("enumField", "firstValue", "secondValue", "thirdValue", "forthValue", "fifthValue")
				.addFile("fileField")
				.addBinary("binaryField")
		;

		Table person = database.addTable("person");
		Table company = database.addTable("company");
		Table contract = database.addTable("contract");
		person
				.addText("firstName")
				.addText("lastName")
				.addReference("company", company, false, "employees");

		company
				.addText("name")
				.addReference("employees", person, true, "company")
				.addReference("companyContracts", contract, true, "companies");

		contract
				.addEnum("contractType", "typeA", "typeB", "typeC")
				.addText("title")
				.addReference("companies", company, true, "companyContracts");

		Table personView = database.addView("personView", person);
		Table personView2 = database.addView("personView2", person);
		Table companyView = database.addView("companyView", company);
		personView
				.addText("firstName")
				.addText("lastName")
				.addReference("company", companyView, false)
		;
		personView2
				.addText("lastName")
				.addReference("company", companyView, false)
		;
		companyView
				.addText("name")
				.addReference("employees", personView, true)
		;

		Table personWithViewRef = database.addTable("personWithViewRef");
		personWithViewRef
				.addText("name")
				.addInteger("value")
				.addReference("companyView", companyView, false)
		;

		Table cascadeTest1 = database.addTable("cascadeTest1", TableOption.CHECKPOINTS, TableOption.HIERARCHY, TableOption.TRACK_CREATION, TableOption.TRACK_MODIFICATION, TableOption.KEEP_DELETED);
		Table cascadeTest2 = database.addTable("cascadeTest2", TableOption.CHECKPOINTS, TableOption.HIERARCHY, TableOption.TRACK_CREATION, TableOption.TRACK_MODIFICATION, TableOption.KEEP_DELETED);

		cascadeTest1.addText("name");
		cascadeTest1.addReference("ref2s", cascadeTest2, true, "ref1", true);

		cascadeTest2.addText("name");
		cascadeTest2.addReference("ref1", cascadeTest1, false, "ref2s", false);


		Table entityA = database.addTable("entityA");
		Table entityB = database.addTable("entityB");

		entityA
				.addText("value")
				.addReference("entityB", entityB, false, "entityA")
		;

		entityB
				.addText("value")
				.addReference("entityA", entityA, false, "entityB")
		;


		return schema;
	}
}
