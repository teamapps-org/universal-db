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
package org.teamapps.universaldb.index.text;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.*;

public class CollectionTextSearchIndexTest {

    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void nearRealTimeSearchAppliesReplacementAndDeletionBeforeCommit() throws Exception {
        CollectionTextSearchIndex index = new CollectionTextSearchIndex(temporary.newFolder(), "files");
        BitSet candidates = new BitSet(); candidates.set(1);
        try {
            index.setRecordValues(1, List.of(new FullTextIndexValue("CONTENT", "altertext")), false);
            assertTrue(index.filter(candidates, List.of(new TextFieldFilter(TextFilterType.TERM_EQUALS, "CONTENT", "altertext")), true).get(1));
            index.setRecordValues(1, List.of(new FullTextIndexValue("CONTENT", "neuertext")), true);
            assertTrue(index.filter(candidates, List.of(new TextFieldFilter(TextFilterType.TERM_EQUALS, "CONTENT", "altertext")), true).isEmpty());
            assertTrue(index.filter(candidates, List.of(new TextFieldFilter(TextFilterType.TERM_EQUALS, "CONTENT", "neuertext")), true).get(1));
            index.delete(1, List.of());
            assertTrue(index.filter(candidates, List.of(new TextFieldFilter(TextFilterType.TERM_EQUALS, "CONTENT", "neuertext")), true).isEmpty());
        } finally { index.commit(true); }
    }

    @Test
    public void getMaxDoc() {
    }

    @Test
    public void deleteAllDocuments() {
    }
}
