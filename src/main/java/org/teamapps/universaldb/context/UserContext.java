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
package org.teamapps.universaldb.context;

import java.text.Collator;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public interface UserContext {

	static Comparator<String> createDefaultComparator(boolean ascending) {
		Collator collator = Collator.getInstance();
		collator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
		collator.setStrength(Collator.PRIMARY);
		return ascending ? Comparator.nullsFirst(collator) : Comparator.nullsLast(collator.reversed());
	}

	static Comparator<String> getOrCreateComparator(UserContext userContext, boolean ascending) {
		return userContext != null ? userContext.getComparator(ascending) : createDefaultComparator(ascending);
	}

	static UserContext create(Locale locale) {
		return new UserContextImpl(locale);
	}

	static UserContext create(String... rankedLanguages) {
		return new UserContextImpl(rankedLanguages);
	}

	static UserContext create(List<String> rankedLanguages) {
		return new UserContextImpl(rankedLanguages);
	}

	default String getLanguage() {
		return getRankedLanguages().getFirst();
	}

	Locale getLocale();

	default Comparator<String> getComparator(boolean ascending) {
		Collator collator = Collator.getInstance(getLocale());
		collator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
		collator.setStrength(Collator.PRIMARY);
		return ascending ? Comparator.nullsFirst(collator) : Comparator.nullsLast(collator.reversed());
	}

	List<String> getRankedLanguages();

}
