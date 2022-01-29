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
package org.teamapps.universaldb.index.reference.value;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface MultiReferenceValue {

	static MultiReferenceValue create(DataInputStream dataInputStream) throws IOException {
		MultiReferenceValueType type = MultiReferenceValueType.getMultiReferenceValueTypeById(dataInputStream.readInt());
		MultiReferenceValue value = null;
		switch (type) {
			case REFERENCE_ITERATOR:
				break;
			case EDIT_VALUE:
				value = new MultiReferenceEditValue();
				break;
		}
		value.readValues(dataInputStream);
		return value;
	}

	MultiReferenceValueType getType();

	void writeValues(DataOutputStream dataOutputStream) throws IOException;

	void readValues(DataInputStream dataInputStream) throws IOException;
}
