/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2022 TeamApps.org
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

public class S3Model implements SchemaInfoProvider {

	public Schema getSchema() {
		Schema schema = Schema.create();
		schema.setSchemaName("TestS3Schema");
		schema.setPojoNamespace("org.teamapps.s3model");
		Database database = schema.addDatabase("s3db");
		Table table = database.addTable("s3Table");
		table
				.addInteger("intField")
				.addLong("longField")
				.addText("textField")
				.addFile("fileField");

		return schema;
	}
}
