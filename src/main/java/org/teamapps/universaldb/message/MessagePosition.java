/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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
package org.teamapps.universaldb.message;

import org.teamapps.message.protocol.message.MessageRecord;

public class MessagePosition<MESSAGE extends MessageRecord> {

	private final int id;
	private final long position;
	private MESSAGE message;

	public MessagePosition(int id, long position) {
		this.id = id;
		this.position = position;
	}

	public int getId() {
		return id;
	}

	public long getPosition() {
		return position;
	}

	public MESSAGE getMessage() {
		return message;
	}

	public void setMessage(MESSAGE message) {
		this.message = message;
	}
}
