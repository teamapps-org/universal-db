package org.teamapps.universaldb.context;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public interface UserContext {

	static UserContext create(Locale locale) {
		return new UserContextImpl(locale);
	}

	static UserContext create(String... rankedLanguages) {
		return new UserContextImpl(rankedLanguages);
	}

	static UserContext create(List<String> rankedLanguages) {
		return new UserContextImpl(rankedLanguages);
	}

	static UserContext create() {
		return new UserContextImpl();
	}

	String getLanguage();

	Locale getLocale();

	Comparator<String> getComparator(boolean ascending);

	List<String> getRankedLanguages();

}
