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
package org.teamapps.universaldb.index.translation;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TranslatableTextTest {

    @Test
    public void getTranslation() {
        TranslatableText text = new TranslatableText("text-en", "en");
        text.setTranslation("text-de", "de");
        text.setTranslation("text-fr", "fr");

        assertEquals("text-en", text.getText("en"));
        assertEquals("text-en", text.getText("xx"));
        assertEquals("text-de", text.getText("de"));
        assertEquals("text-fr", text.getText("fr"));

        assertTrue(text.getTranslationMap().containsKey("de"));
        assertTrue(text.getTranslationMap().containsKey("en"));


        String encodedValue = text.getEncodedValue();
        assertNotNull(encodedValue);
        text = new TranslatableText(encodedValue);
        assertEquals("text-en", text.getText("en"));
        assertEquals("text-en", text.getText("xx"));
        assertEquals("text-de", text.getText("de"));
        assertEquals("text-fr", text.getText("fr"));

        assertTrue(text.getTranslationMap().containsKey("de"));
        assertTrue(text.getTranslationMap().containsKey("en"));

    }

    @Test
    public void setTranslation() {
    }

    @Test
    public void translationLookup() {
    }

    @Test
    public void getEncodedValue() {
    }

	@Test
	public void testGetEncodedValue() {
	}


    public void compareImplementations() throws IOException {
        int loops = 10;
        int size = 1_000_000;
        for (int n = 0; n < loops; n++) {
            long time = System.currentTimeMillis();
            List<String> encodedList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                TranslatableText translatableText = new TranslatableText("text" + i, "de").setTranslation("text-en" + i, "en").setTranslation("text-fr" + i, "fr");
                String encoded = translatableText.getEncodedValue();
                encodedList.add(encoded);
            }
            System.out.println("Time encode: " + (System.currentTimeMillis() - time));
            time = System.currentTimeMillis();
            for (int i = 0; i < size; i++) {
                String value = encodedList.get(i);
                TranslatableText translatableText = new TranslatableText(value);
                if (!("text-en" + i).equals(translatableText.getText("en"))) {
                    System.out.println("Error:" + translatableText.getText("en"));
                }
                if (!("text" + i).equals(translatableText.getText())) {
                    System.out.println("Error:" + translatableText.getText());
                }
            }
            System.out.println("Time decode: " + (System.currentTimeMillis() - time));
        }

        System.out.println("Binary:...");
        for (int n = 0; n < loops; n++) {
            long time = System.currentTimeMillis();
            List<byte[]> encodedList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                BinaryTranslatedText translatableText = new BinaryTranslatedText("text" + i, "de").setTranslation("text-en" + i, "en").setTranslation("text-fr" + i, "fr");
                byte[] encoded = translatableText.getEncodedValue();
                encodedList.add(encoded);
            }
            System.out.println("Time encode: " + (System.currentTimeMillis() - time));
            time = System.currentTimeMillis();
            for (int i = 0; i < size; i++) {
                byte[] value = encodedList.get(i);
                BinaryTranslatedText translatableText = new BinaryTranslatedText(value);
                if (!("text-en" + i).equals(translatableText.getText("en"))) {
                    System.out.println("Error:" + translatableText.getText("en"));
                }
                if (!("text" + i).equals(translatableText.getText())) {
                    System.out.println("Error:" + translatableText.getText());
                }
            }
            System.out.println("Time decode: " + (System.currentTimeMillis() - time));
        }

    }
}
