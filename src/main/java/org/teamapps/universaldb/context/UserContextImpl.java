package org.teamapps.universaldb.context;

import java.text.Collator;
import java.util.*;

public class UserContextImpl implements UserContext{

	private String language;
	private Locale locale;
	private List<String> rankedLanguages;

	public UserContextImpl(Locale locale) {
		this.locale = locale;
		this.language = locale.getLanguage();
		this.rankedLanguages = Collections.singletonList(language);
	}

	public UserContextImpl(String... rankedLanguages) {
		this(Arrays.asList(rankedLanguages));
	}

	public UserContextImpl(List<String> rankedLanguages) {
		if (rankedLanguages == null || rankedLanguages.isEmpty()) {
			rankedLanguages = Collections.singletonList("en");
		}
		this.rankedLanguages = rankedLanguages;
		language = rankedLanguages.get(0);
		locale = Locale.forLanguageTag(language);
	}

	@Override
	public String getLanguage() {
		return language;
	}

	@Override
	public Locale getLocale() {
		return locale;
	}

	@Override
	public Comparator<String> getComparator(boolean ascending) {
		Collator collator = Collator.getInstance(locale);
		collator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
		collator.setStrength(Collator.PRIMARY);
		return ascending ? Comparator.nullsFirst(collator) : Comparator.nullsLast(collator.reversed());
	}

	@Override
	public List<String> getRankedLanguages() {
		return rankedLanguages;
	}
}
