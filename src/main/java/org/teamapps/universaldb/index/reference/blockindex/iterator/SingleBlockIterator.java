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
package org.teamapps.universaldb.index.reference.blockindex.iterator;

import org.teamapps.universaldb.index.reference.blockindex.ReferenceBlock;
import org.teamapps.universaldb.index.reference.blockindex.ReferenceBuffer;

import java.util.PrimitiveIterator;

public class SingleBlockIterator implements PrimitiveIterator.OfInt {
	private final int blockEntries;
	private final int offset;
	private final ReferenceBuffer buffer;
	private int pos;
	private int nextValue;

	public SingleBlockIterator(ReferenceBlock block) {
		this.blockEntries = block.getBlockEntryCount();
		this.offset = block.getBlockDataPosition();
		this.buffer = block.getBuffer();
		retrieveNextValue();
	}

	private void retrieveNextValue() {
		if (pos >= blockEntries) {
			nextValue = 0;
			return;
		}
		nextValue = buffer.readInt(offset + pos * 4);
		pos++;
		if (nextValue < 0) {
			retrieveNextValue();
		}
	}

	@Override
	public int nextInt() {
		int value = nextValue;
		retrieveNextValue();
		return value;
	}

	@Override
	public boolean hasNext() {
		return nextValue > 0;
	}
}
