/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class UserContextImpl implements UserContext {

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
	public List<String> getRankedLanguages() {
		return rankedLanguages;
	}
}
