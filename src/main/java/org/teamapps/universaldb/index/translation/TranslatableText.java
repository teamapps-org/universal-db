package org.teamapps.universaldb.index.translation;

import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class TranslatableText {

    private final static String DELIMITER = "\n<=@#!=>\n";

    private String originalText;
    private String originalLanguage;
    private String encodedValue;
    private Map<String, String> translationMap;

    public static TranslatableText create(String originalText, String originalLanguage) {
        return new TranslatableText(originalText, originalLanguage);
    }

    public TranslatableText() {
    }

    public TranslatableText(String originalText, String originalLanguage) {
        this.originalText = originalText;
        this.originalLanguage = originalLanguage;
    }

    public TranslatableText(String encodedValue) {
        if (encodedValue != null && (!encodedValue.startsWith(DELIMITER) || !encodedValue.endsWith(DELIMITER))) {
            throw new RuntimeException("Error: invalid translation encoding:" + encodedValue);
        }
        this.encodedValue = encodedValue;
    }

    public TranslatableText(DataInputStream dataInputStream) throws IOException {
        encodedValue = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
    }

    public TranslatableText(String originalText, String originalLanguage, Map<String, String> translationMap) {
        if (translationMap.keySet().stream().anyMatch(s -> s.length() != 2)) {
            throw new RuntimeException("Error: invalid translation map");
        }
        this.originalText = originalText;
        this.originalLanguage = originalLanguage;
        this.translationMap = translationMap;
    }

    public String getText() {
        if (originalText == null) {
            parseOriginalValue();
        }
        return originalText;
    }

    public String getOriginalLanguage() {
        if (originalLanguage == null) {
            parseOriginalValue();
        }
        return originalLanguage;
    }

    public String getText(String language) {
        String translation = getTranslation(language);
        return translation != null ? translation : getText();
    }

    public String getText(List<String> rankedLanguages) {
        String translation = getTranslation(rankedLanguages);
        return translation != null ? translation : getText();
    }

    public String getTranslation(String language) {
        if (translationMap != null) {
            if (language.equals(originalLanguage)) {
                return originalText;
            }
            return translationMap.get(language);
        } else {
            return translationLookup(language);
        }
    }

    public String getTranslation(List<String> rankedLanguages) {
        for (String language : rankedLanguages) {
            String translation = getTranslation(language);
            if (translation != null) {
                return translation;
            }
        }
        return null;
    }

    public TranslatableText setTranslation(String translation, String language) {
        if (translation == null || translation.isEmpty() || language == null || language.length() != 2) {
            return this;
        }
        getTranslationMap().put(language, translation);
        return this;
    }

    public String translationLookup(String language) {
        return findTranslation(language);
    }

    public String getEncodedValue() {
        if (translationMap != null || originalText != null) {
            return createTranslationValue(originalText, originalLanguage, translationMap);
        } else {
            return encodedValue;
        }
    }

    private void parseOriginalValue() {
        if (encodedValue == null) {
            return;
        }
        int pos = -1;
        if((pos = encodedValue.indexOf(DELIMITER, pos + 1)) >= 0 && pos < encodedValue.length() - 7) {
            int end = encodedValue.indexOf(DELIMITER, pos + 1);
            if (end > pos) {
                originalLanguage = encodedValue.substring(pos + DELIMITER.length(), pos + DELIMITER.length() + 2);
                originalText = encodedValue.substring(pos +  DELIMITER.length() + 3, end);
            }
        }
    }

    private String findTranslation( String language) {
        if (encodedValue == null || language == null || language.length() != 2) {
            return null;
        }
        if (language.equalsIgnoreCase(originalLanguage)) {
            return originalText;
        }
        int pos = -1;
        char a = language.charAt(0);
        char b = language.charAt(1);
        while((pos = encodedValue.indexOf(DELIMITER, pos + 1)) >= 0 && pos < encodedValue.length() - DELIMITER.length()) {
            if (encodedValue.charAt(pos + DELIMITER.length()) == a && encodedValue.charAt(pos + DELIMITER.length() + 1) == b) {
                int end = encodedValue.indexOf(DELIMITER, pos + 1);
                if (end > pos) {
                    return encodedValue.substring(pos + DELIMITER.length() + 3, end);
                }
            }
        }
        return null;
    }

    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        String encodedValue = getEncodedValue();
        DataStreamUtil.writeStringWithLengthHeader(dataOutputStream, encodedValue);
    }

    public Map<String, String> getTranslationMap() {
        if (translationMap != null) {
            return translationMap;
        } else {
            translationMap = encodedValue != null ? parseEncodedTranslation(encodedValue) : new HashMap<>();
            return translationMap;
        }
    }

    private Map<String, String> parseEncodedTranslation(String text) {
        Map<String, String> map = new HashMap<>();
        int pos = -1;
        while((pos = text.indexOf(DELIMITER, pos + 1)) >= 0 && pos < text.length() - 7) {
            int end = text.indexOf(DELIMITER, pos + 1);
            if (end > pos) {
                String language = text.substring(pos + DELIMITER.length(), pos + DELIMITER.length() + 2);
                String value = text.substring(pos +  DELIMITER.length() + 3, end);
                map.put(language, value);
                pos = end - 1;
            }
        }
        return map;
    }

    private static String createTranslationValue(String originalText, String originalLanguage, Map<String, String> translationsByLanguage) {
        StringBuilder sb = new StringBuilder();
        if (originalText != null && originalLanguage != null) {
            sb.append(DELIMITER).append(originalLanguage).append(":").append(originalText);
        }
        if (translationsByLanguage != null) {
            for (Map.Entry<String, String> entry : translationsByLanguage.entrySet()) {
                sb.append(DELIMITER).append(entry.getKey()).append(":").append(entry.getValue());
            }
        }
        sb.append(DELIMITER);
        return sb.toString();
    }

}
