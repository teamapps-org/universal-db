package org.teamapps.universaldb.message;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

	default void closeSave() {
		try {
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
