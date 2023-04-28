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
package org.teamapps.universaldb.pojo.template;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TemplateUtil {

	public static final String ENTITY_INTERFACE_TPL = "EntityInterface.txt";
	public static final String ENTITY_VIEW_INTERFACE_TPL = "EntityViewInterface.txt";
	public static final String QUERY_INTERFACE_TPL = "QueryInterface.txt";
	public static final String UDB_ENTITY_TPL = "UdbEntity.txt";
	public static final String UDB_ENTITY_VIEW_TPL = "UdbEntityView.txt";
	public static final String UDB_QUERY_TPL = "UdbQuery.txt";
	public static final String ENUM_TPL = "Enum.txt";

	public static final String MODEL_PROVIDER_TPL = "ModelProvider.txt";
	public static final String TEMPLATE_BLOCKS_TPL = "templateBlocks.txt";

	public static String readeTemplate(String name) throws IOException {
		BufferedReader reader = getBufferedReader(name);
		String line;
		StringBuilder sb = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			sb.append(line).append("\n");
		}
		return sb.toString();
	}

	public static Map<String, String> readTemplateBlocks(String name) throws IOException {
		Map<String, String> blockMap = new HashMap<>();
		BufferedReader reader = getBufferedReader(name);
		String line;
		String multiLineName = null;
		String singleLineName = null;
		List<String> lines = new ArrayList<>();
		while ((line = reader.readLine()) != null) {
			if (multiLineName != null && line.startsWith("#")) {
				blockMap.put(multiLineName, lines.stream().collect(Collectors.joining("\n")));
				multiLineName = null;
				lines.clear();
			}
			if (multiLineName != null) {
				lines.add(line);
			} else if (singleLineName != null) {
				blockMap.put(singleLineName, line);
				singleLineName = null;
			} else {
				if (line.startsWith("{") && line.endsWith("}#")) {
					multiLineName = line.substring(1, line.indexOf('}'));
				} else if (line.startsWith("{") && line.endsWith("}:")) {
					singleLineName = line.substring(1, line.indexOf('}'));
				}
			}
		}
		return blockMap;
	}

	public static BufferedReader getBufferedReader(String name) {
		InputStream is = TemplateUtil.class.getResourceAsStream("/org/teamapps/pojo/template/" + name);
		return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
	}

	public static String getEntityInterfaceTemplate() throws IOException {
		return readeTemplate(ENTITY_INTERFACE_TPL);
	}

	public static String getModelProviderTemplate() throws IOException {
		return readeTemplate(MODEL_PROVIDER_TPL);
	}

	public static String getEnumTemplate() throws IOException {
		return readeTemplate(ENUM_TPL);
	}

	public static Map<String, String>  getTemplateBlocksMap() {
		try {
			return readTemplateBlocks(TEMPLATE_BLOCKS_TPL);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String setValue(String template, String name, String value) {
		return template.replace("{" + name + "}", value);
	}

	public static String firstUpperCase(String value) {
		return value.substring(0, 1).toUpperCase() + value.substring(1 );
	}

}
