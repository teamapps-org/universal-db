/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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

import org.teamapps.universaldb.model.DatabaseModel;
import org.teamapps.universaldb.model.EnumModel;
import org.teamapps.universaldb.model.ReferenceFieldModel;
import org.teamapps.universaldb.model.TableModel;
import org.teamapps.universaldb.schema.ModelProvider;

public class Model implements ModelProvider {

	@Override
	public DatabaseModel getModel() {
		DatabaseModel model = new DatabaseModel("testDb1", "testDb1", "org.teamapps.datamodel");

		EnumModel enumModel = model.createEnum("enumField", "firstValue", "secondValue", "thirdValue", "forthValue", "fifthValue");
		EnumModel contractEnum = model.createEnum("contractType", "typeA", "typeB", "typeC");

		TableModel table = model.createTable("fieldTest");
		TableModel person = model.createTable("person");
		TableModel company = model.createTable("company");
		TableModel contract = model.createTable("contract");

		table.addBoolean("booleanField");
		table.addShort("shortField");
		table.addInteger("intField");
		table.addLong("longField");
		table.addFloat("floatField");
		table.addDouble("doubleField");
		table.addText("textField");
		table.addTranslatableText("translatableText");
		table.addTimestamp("timestampField");
		table.addTime("timeField");
		table.addDate("dateField");
		table.addDateTime("dateTimeField");
		table.addLocalDate("localDateField");
		table.addEnum("enumField", enumModel);
		table.addFile("fileField");
		table.addByteArray("binaryField");
		ReferenceFieldModel singleReferenceField = table.addReference("singleReferenceField", table);
		table.addReference("backRefSingleReferenceField", singleReferenceField);
		table.addReference("singleReferenceNoBackRefField", table, false);
		ReferenceFieldModel multiReferenceField = table.addMultiReference("multiReferenceField", table);
		table.addMultiReference("backRefMultiReferenceField", multiReferenceField);
		ReferenceFieldModel parent = table.addReference("parent", table);
		table.addMultiReference("children", parent);


		person.addText("firstName");
		person.addText("lastName");
		ReferenceFieldModel companyRef = person.addReference("company", company);//, false, "employees"

		company.addText("name");
		company.addMultiReference("employees", companyRef);
		ReferenceFieldModel companyContracts = company.addMultiReference("companyContracts", contract);

		contract.addEnum("contractType", contractEnum);
		contract.addText("title");
		contract.addMultiReference("companies", companyContracts);

//		return model;

//		Schema schema = Schema.create();
//		schema.setSchemaName("TestBaseSchema");
//		Database database = schema.addDatabase("testDb1");
//
//		Table fieldTestView = database.addView("fieldTestView", table);
//
//		Table personView = database.addView("personView", person);
//		Table personView2 = database.addView("personView2", person);
//		Table companyView = database.addView("companyView", company);
//		personView
//				.addText("firstName")
//				.addText("lastName")
//				.addReference("company", companyView, false)
//		;
//		personView2
//				.addText("lastName")
//				.addReference("company", companyView, false)
//		;
//		companyView
//				.addText("name")
//				.addReference("employees", personView, true)
//		;
//
//		Table personWithViewRef = database.addTable("personWithViewRef");
//		personWithViewRef
//				.addText("name")
//				.addInteger("value")
//				.addReference("companyView", companyView, false)
//		;
//


		TableModel cascadeTest1 = model.createTable("cascadeTest1");
		TableModel cascadeTest2 = model.createTable("cascadeTest2");
		TableModel cascadeTest3 = model.createTable("cascadeTest3NoKeep", "cascadeTest3NoKeep", true, false, false);
		TableModel cascadeTest4 = model.createTable("cascadeTest4NoKeep", "cascadeTest4NoKeep", false, false, false);
		TableModel cascadeTest5 = model.createTable("cascadeTest5NoKeep", "cascadeTest5NoKeep", false, false, false);

		cascadeTest2.addText("name");
		ReferenceFieldModel t2Ref1 = cascadeTest2.addReference("ref1", cascadeTest1);
		ReferenceFieldModel t2OtherRef1 = cascadeTest2.addReference("otherRef1", cascadeTest1);

		cascadeTest3.addText("name");
		ReferenceFieldModel t3Ref1 = cascadeTest3.addReference("ref1", cascadeTest1);

		cascadeTest4.addText("name");
		ReferenceFieldModel t4Ref1 = cascadeTest4.addReference("ref1", cascadeTest1);

		cascadeTest5.addText("name");
		ReferenceFieldModel t5Ref1 = cascadeTest5.addReference("ref1", cascadeTest1, true);


		cascadeTest1.addText("name");
		cascadeTest1.addMultiReference("ref2s", t2Ref1, true);
		cascadeTest1.addMultiReference("otherRef2s", t2OtherRef1, false);
		cascadeTest1.addReference("ref3NoKeep", t3Ref1, true);
		cascadeTest1.addReference("ref4NoKeep", t4Ref1, true);
		cascadeTest1.addReference("ref5NoKeep", t5Ref1, false);


		TableModel entityA = model.createTable("entityA");
		TableModel entityB = model.createTable("entityB");

		entityA.addText("value");
		ReferenceFieldModel entityBRef = entityA.addReference("entityB", entityB);

		entityB.addText("value");
		entityB.addReference("entityA", entityBRef);
		;

		return model;

	}
}
