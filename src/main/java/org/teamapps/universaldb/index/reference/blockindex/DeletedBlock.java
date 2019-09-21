/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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
package org.teamapps.universaldb.index.reference.blockindex;

public class DeletedBlock {

	private static final long MIN_REUSE_DELAY = 1000 * 60 * 15;

	private final ReferenceBlock block;
	private final long timeDeleted;

	public DeletedBlock(ReferenceBlock block) {
		this.block = block;
		this.timeDeleted = System.currentTimeMillis();
	}

	public ReferenceBlock getBlock() {
		return block;
	}

	public long getTimeDeleted() {
		return timeDeleted;
	}

	public boolean isAvailable(long time) {
		return time - timeDeleted >= MIN_REUSE_DELAY;
	}
}
