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
package org.teamapps.universaldb.index.counter;

import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;

public class ViewCounterImpl implements ViewCounter{

	private PrimitiveEntryAtomicStore atomicStore;

	public ViewCounterImpl(TableIndex tableIndex) {
		atomicStore = new PrimitiveEntryAtomicStore(tableIndex.getDataPath(), "viewCounter.vdx");
	}

	@Override
	public int getViews(int id) {
		return atomicStore.getInt(id);
	}

	@Override
	public void addView(int id) {
		atomicStore.setInt(id, getViews(id) + 1);
	}
}
