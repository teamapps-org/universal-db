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
package org.teamapps.universaldb.index.log;

import java.util.ArrayList;
import java.util.List;

public interface LogIndex {

	default long writeLog(byte[] bytes) {
		return writeLog(bytes, true);
	}

	long writeLog(byte[] bytes, boolean committed);

	byte[] readLog(long pos);

	LogIterator readLogs();

	LogIterator readLogs(long pos);

	long[] readLogPositions();

	default List<byte[]> readAllLogs() {
		LogIterator logIterator = readLogs();
		List<byte[]> logs = new ArrayList<>();
		while (logIterator.hasNext()) {
			logs.add(logIterator.next());
		}
		try {
			logIterator.close();
		} catch (Exception e) {
			throw new RuntimeException("Error closing log iterator", e);
		}
		return logs;
	}

	long getPosition();

	boolean isEmpty();

	void flush();

	void close();

	void drop();
}
