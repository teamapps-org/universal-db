/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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

import java.util.*;

public class BaseMessageCache {

	public static BaseMessageCache fullCache() {
		return new BaseMessageCache(true, 0);
	}

	public static BaseMessageCache lruCache(int size) {
		return new BaseMessageCache(false, size);
	}

	private final boolean fullCache;
	private final Map<Integer, MessageRecord> messageMap;
	private final List<MessageRecord> messageList;

	private BaseMessageCache(boolean fullCache, int maxSize) {
		this.fullCache = fullCache;
		messageMap = !fullCache ? new LinkedHashMap<>() {
			@Override
			protected boolean removeEldestEntry(Map.Entry<Integer, MessageRecord> eldest) {
				return size() >= maxSize;
			}
		} : new HashMap<>();
		messageList = fullCache ? new ArrayList<>() : null;
	}

	public void addMessage(int id, boolean update, MessageRecord message) {
		if (fullCache) {
			if (update) {
				MessageRecord oldMessage = messageMap.get(id);
				if (oldMessage != null) {
					messageList.remove(oldMessage);
				}
			}
			messageList.add(message);
		}
		messageMap.put(id, message);
	}

	public void removeMessage(int id) {
		MessageRecord oldMessage = messageMap.remove(id);
		if (fullCache && oldMessage != null) {
			messageList.remove(oldMessage);
		}
	}

	public MessageRecord getMessage(int id) {
		return messageMap.get(id);
	}

	public List<MessageRecord> getMessages() {
		return new ArrayList<>(messageList);
	}

	public int getMessageCount() {
		return messageList.size();
	}

	public boolean isFullCache() {
		return fullCache;
	}
}
