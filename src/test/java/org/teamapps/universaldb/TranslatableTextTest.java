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
package org.teamapps.universaldb;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.FieldTest;
import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.index.translation.TranslatableTextFilter;

import static org.junit.Assert.assertEquals;

public class TranslatableTextTest {

    @BeforeClass
    public static void init() throws Exception {
        TestBase.init();
    }

    @Test
    public void testLargeText() {
        TranslatableText translatableText = TranslatableText.create("en-text", "en")
                .setTranslation("de-text", "de")
                .setTranslation("fr-text", "fr");
        FieldTest.create()
                .setTextField("ID1")
                .setTranslatableText(translatableText)
                .save();

        UserContext context = UserContext.create("fr", "de");
//        UserContext context = UserContext.create("de");
        FieldTest fieldTest = FieldTest.filter()
                .translatableText(TranslatableTextFilter.termContainsFilter("fr", context))
                .executeExpectSingleton();

        assertEquals("ID1", fieldTest.getTextField());
    }
}
