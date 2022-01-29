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
package org.teamapps.universaldb.index;

public class FullTextIndexingOptions {

	public static FullTextIndexingOptions INDEXED = new FullTextIndexingOptions(true, true, false, false, false);
	public static FullTextIndexingOptions NOT_INDEXED = new FullTextIndexingOptions(false, false, false, false, false);

	private boolean index;
	private boolean analyzeContent;
	private boolean positionIndex;
	private boolean offsetIndex;
	private boolean storeContent;

	protected FullTextIndexingOptions(boolean index, boolean analyzeContent, boolean positionIndex, boolean offsetIndex, boolean storeContent) {
		this.index = index;
		this.analyzeContent = analyzeContent;
		this.positionIndex = positionIndex;
		this.offsetIndex = offsetIndex;
		this.storeContent = storeContent;
	}

	public boolean isIndex() {
		return index;
	}

	public boolean isAnalyzeContent() {
		return analyzeContent;
	}

	public boolean isPositionIndex() {
		return positionIndex;
	}

	public boolean isOffsetIndex() {
		return offsetIndex;
	}

	public boolean isStoreContent() {
		return storeContent;
	}
}
