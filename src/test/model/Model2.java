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

public class Model2 implements ModelProvider {

	@Override
	public DatabaseModel getModel() {
		DatabaseModel model = new DatabaseModel("testDb2", "testDb2", "org.teamapps.datamodel");

		EnumModel enumModel = model.createEnum("enumField", "firstValue", "secondValue", "thirdValue", "forthValue", "fifthValue");
		EnumModel contractEnum = model.createEnum("contractType", "typeA", "typeB", "typeC");

		TableModel table = model.createRemoteTable("fieldTest", "testDb1");
		TableModel person = model.createRemoteTable("person", "testDb1");
		TableModel company = model.createRemoteTable("company", "testDb1");
		TableModel contract = model.createRemoteTable("contract", "testDb1");

		TableModel model2Table = model.createTable("model2Table");

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


		model2Table.addText("name");
		model2Table.addReference("person", person);
		model2Table.addReference("company", company);
		model2Table.addMultiReference("contracts", contract);

		return model;

	}
}
