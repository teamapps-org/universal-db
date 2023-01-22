package org.teamapps.universaldb.message;

import org.teamapps.message.protocol.message.Message;

import java.util.*;

public class MessageCache<MESSAGE extends Message> {

	public static MessageCache fullCache() {
		return new MessageCache(true, 0);
	}

	public static MessageCache lruCache(int size) {
		return new MessageCache(false, size);
	}

	private final boolean fullCache;
	private final Map<Integer, MESSAGE> messageMap;
	private final List<MESSAGE> messageList;

	private MessageCache(boolean fullCache, int maxSize) {
		this.fullCache = fullCache;
		messageMap = !fullCache ? new LinkedHashMap<>() {
			@Override
			protected boolean removeEldestEntry(Map.Entry<Integer, MESSAGE> eldest) {
				return size() >= maxSize;
			}
		} : new HashMap<>();
		messageList = fullCache ? new ArrayList<>() : null;
	}

	public void addMessage(int id, boolean update, MESSAGE message) {
		if (fullCache) {
			if (update) {
				MESSAGE oldMessage = messageMap.get(id);
				messageList.remove(oldMessage);
			}
			messageList.add(message);
		}
		messageMap.put(id, message);
	}

	public void removeMessage(int id) {
		MESSAGE oldMessage = messageMap.remove(id);
		if (fullCache && oldMessage != null) {
			messageList.remove(oldMessage);
		}
	}

	public MESSAGE getMessage(int id) {
		return messageMap.get(id);
	}

	public List<MESSAGE> getMessages() {
		return new ArrayList<>(messageList);
	}

	public int getMessageCount() {
		return messageList.size();
	}

	public boolean isFullCache() {
		return fullCache;
	}
}
