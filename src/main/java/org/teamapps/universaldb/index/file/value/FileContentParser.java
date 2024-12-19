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
package org.teamapps.universaldb.index.file.value;

import com.github.pemistahl.lingua.api.IsoCode639_1;
import com.github.pemistahl.lingua.api.Language;
import com.github.pemistahl.lingua.api.LanguageDetector;
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.WriteLimitReachedException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.teamapps.udb.model.FileContentData;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class FileContentParser {

	public static TikaConfig TIKA_CONFIG;
	private static final LanguageDetector languageDetector = LanguageDetectorBuilder.fromAllLanguages().withLowAccuracyMode().build();
	private final File file;
	private final String fileName;
	private Metadata meta;
	private FileContentData data;

	static {
		try {
			String xmlConfig = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
					"<properties>\n" +
					"   <parsers>\n" +
					"       <parser class=\"org.apache.tika.parser.DefaultParser\">\n" +
					"           <parser-exclude class=\"org.apache.tika.parser.external.CompositeExternalParser\"/>\n" +
					"           <parser-exclude class=\"org.apache.tika.parser.executable.ExecutableParser\"/>\n" +
					"           <parser-exclude class=\"org.apache.tika.parser.ocr.TesseractOCRParser\"/>\n" +
					"       </parser>\n" +
					"   </parsers>\n" +
					"</properties>";
			TIKA_CONFIG = new TikaConfig(new ByteArrayInputStream(xmlConfig.getBytes(StandardCharsets.UTF_8)));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static FileContentData parseFile(File file) {
		return parseFile(file, file.getName());
	}

	public static FileContentData parseFile(File file, String fileName) {
		FileContentParser fileContentParser = new FileContentParser(file, fileName);
		return fileContentParser.getFileContentData(100_000);
	}

	public FileContentParser(File file, String fileName) {
		this.file = file;
		this.fileName = fileName != null ? fileName : file.getName();
		data = new FileContentData();
		data.setName(fileName);
		data.setFileSize(file.length());
		data.setHash(createFileHash(file));
	}

	public String getHash() {
		return data.getHash();
	}

	public FileContentData getFileContentData(int maxContentLength) {
		if (meta == null) {
			parseMetaData(maxContentLength);
		}
		return data;
	}

	public String getContentLanguage() {
		getFileContentData(100_000);
		if (data.getLanguage() == null) {
			detectLanguage();
		}
		return data.getLanguage();
	}

	private void parseMetaData(int maxContentLength) {
		try {
			BodyContentHandler handler = new BodyContentHandler(maxContentLength);
			meta = new Metadata();
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			Parser parser = new AutoDetectParser(TIKA_CONFIG);
			try {
				parser.parse(bis, handler, meta, new ParseContext());
			} catch (WriteLimitReachedException ignore) {
			}
			data.setContent(handler.toString());

			List<String> propertyNames = new ArrayList<>();
			List<String> propertyValues = new ArrayList<>();
			for (String name : Arrays.stream(meta.names()).filter(name -> !name.startsWith("X-TIKA:")).sorted(String::compareTo).toList()) {
				propertyNames.add(name);
				propertyValues.add(meta.get(name));
			}
			data.setMetaKeysAsList(propertyNames);
			data.setMetaValuesAsList(propertyValues);

			data.setMimeType(properties("Content-Type").getString());
			data.setCreatedBy(properties(TikaCoreProperties.CREATOR).properties("xmpDM:artist").getString());
			data.setModifiedBy(properties(TikaCoreProperties.MODIFIER).getString());
			data.setDateCreated(properties(TikaCoreProperties.CREATED).getInstant());
			data.setDateModified(properties(TikaCoreProperties.MODIFIED).getInstant());
			Integer pages = properties(Office.PAGE_COUNT, Office.SLIDE_COUNT).properties("meta:page-count", "xmpTPg:NPages", "meta:slide-count").getInt();
			if (pages != null) {
				data.setPages(pages);
			}
			data.setTitle(properties(TikaCoreProperties.TITLE, TikaCoreProperties.SUBJECT).properties("dc:title", "xmpDM:album").getString());
			data.setLatitude(properties(TikaCoreProperties.LATITUDE).properties("geo:lat").getString());
			data.setLongitude(properties(TikaCoreProperties.LONGITUDE).properties("geo:long").getString());
			data.setDevice(properties("tiff:Model", "Exif IFD0:Model", "Exif SubIFD:Lens Model").getString());
			data.setSoftware(properties("extended-properties:Application", "pdf:docinfo:creator_tool", "pdf:producer", "pdf:docinfo:producer").getString());
			data.setSoftwareVersion(properties("extended-properties:AppVersion").getString());
			data.setDuration(properties("xmpDM:duration").getString());
			Integer imageWidth = properties("tiff:ImageWidth").getInt();
			if (imageWidth != null) {
				data.setImageWidth(imageWidth);
			}
			Integer imageHeight = properties("tiff:ImageLength").getInt();
			if (imageHeight != null) {
				data.setImageHeight(imageHeight);
			}
		} catch (Throwable ignore) {
		}
	}

	private void detectLanguage() {
		String content = data.getContent();
		if (content != null && content.length() > 100) {
			Language language = languageDetector.detectLanguageOf(content);
			if (language.getIsoCode639_1() != IsoCode639_1.NONE) {
				data.setLanguage(language.getIsoCode639_1().name().toLowerCase());
			}
		}
	}

	private PropertyReader properties(Property... properties) {
		return new PropertyReader(meta).properties(properties);
	}

	private PropertyReader properties(String... properties) {
		return new PropertyReader(meta).properties(properties);
	}

	private static class PropertyReader {
		private final Metadata meta;
		private final List<Property> properties = new ArrayList<>();
		private final List<String> otherProperties = new ArrayList<>();

		public PropertyReader(Metadata meta) {
			this.meta = meta;
		}

		public PropertyReader properties(Property... properties) {
			this.properties.addAll(Arrays.asList(properties));
			return this;
		}

		public PropertyReader properties(String... properties) {
			this.otherProperties.addAll(Arrays.asList(properties));
			return this;
		}

		public String getString() {
			for (Property property : properties) {
				String value = meta.get(property);
				if (value != null && !value.isBlank()) {
					return value;
				}
			}
			for (String property : otherProperties) {
				String value = meta.get(property);
				if (value != null && !value.isBlank()) {
					return value;
				}
			}
			return null;
		}

		public Integer getInt() {
			for (Property property : properties) {
				Integer value = meta.getInt(property);
				if (value != null) {
					return value;
				}
			}
			for (String property : otherProperties) {
				String value = meta.get(property);
				if (value != null && !value.isBlank()) {
					try {
						return Integer.valueOf(value);
					} catch (NumberFormatException ignore) {
					}
				}
			}
			return null;
		}

		public Instant getInstant() {
			for (Property property : properties) {
				Date value = meta.getDate(property);
				if (value != null) {
					return value.toInstant();
				}
			}
			return null;
		}
	}

	public static String bytesToHex(byte[] bytes) {
		char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}

	public static String createFileHash(File file) {
		try {
			byte[] buffer = new byte[8192];
			int count;
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			while ((count = bis.read(buffer)) > 0) {
				digest.update(buffer, 0, count);
			}
			bis.close();
			byte[] hash = digest.digest();
			return bytesToHex(hash);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
