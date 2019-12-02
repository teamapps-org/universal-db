package org.teamapps.universaldb.index.translation;

import org.junit.Test;

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

        String encodedValue = text.getEncodedValue();
        assertNotNull(encodedValue);
        TranslatableText parsedText = new TranslatableText(encodedValue);
        assertEquals("text-en", text.getText("en"));
        assertEquals("text-en", text.getText("xx"));
        assertEquals("text-de", text.getText("de"));
        assertEquals("text-fr", text.getText("fr"));

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
}