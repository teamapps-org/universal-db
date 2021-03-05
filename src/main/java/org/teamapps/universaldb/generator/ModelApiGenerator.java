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
package org.teamapps.universaldb.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.schema.SchemaInfoProvider;

import java.io.File;

public class ModelApiGenerator {

	private static final Logger log = LoggerFactory.getLogger(ModelApiGenerator.class);

	public static void main(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			log.error("Error: missing argument(s). Mandatory arguments are: modelClassName targetPath");
			System.exit(1);
		}
		String schemaClassName = args[0];
		String targetPath = args[1];

		Class<?> schemaClass = Class.forName(schemaClassName);
		SchemaInfoProvider schemaInfoProvider = (SchemaInfoProvider) schemaClass.getConstructor().newInstance();
		Schema schema = schemaInfoProvider.getSchema();
		PojoCodeGenerator pojoCodeGenerator = new PojoCodeGenerator();
		File basePath = new File(targetPath);
		if (!basePath.getParentFile().exists() && basePath.getParentFile().getParentFile().exists()) {
			basePath.getParentFile().mkdir();
		}
		pojoCodeGenerator.generateCode(schema, basePath);
	}

}
