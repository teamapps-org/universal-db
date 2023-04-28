package org.teamapps.universaldb.index.file2;

import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.FileMetaDataEntry;
import org.teamapps.universaldb.index.text.FullTextIndexValue;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Supplier;

public class FileValue {

	private final String fileName;
	private final long size;
	private final String hash;
	private FileContentData contentData;
	private FileContentParser contentParser;
	private Supplier<FileContentData> contentDataSupplier;
	private File file;
	private Supplier<InputStream> inputStreamSupplier;

	public FileValue(File file) {
		this(file, file.getName());
	}
	public FileValue(File file, String fileName) {
		this.file = file;
		this.fileName = fileName;
		this.size = file.length();
		this.contentParser = new FileContentParser(file, fileName);
		this.hash = contentParser.getHash();
	}

	public FileValue(String fileName, long size, byte[] hash, Supplier<FileContentData> contentDataSupplier, Supplier<InputStream> inputStreamSupplier) {
		this.fileName = fileName;
		this.size = size;
		this.hash = HexFormat.of().formatHex(hash);
		this.contentDataSupplier = contentDataSupplier;
		this.inputStreamSupplier = inputStreamSupplier;
	}

	public InputStream getInputStream() throws IOException {
		if (file != null) {
			return new BufferedInputStream(new FileInputStream(file));
		} else {
			return inputStreamSupplier.get();
		}
	}

	public String getFileName() {
		return fileName;
	}

	public long getSize() {
		return size;
	}

	public String getHash() {
		return hash;
	}

	public byte[] getHashBytes() {
		return HexFormat.of().parseHex(hash);
	}

	public FileContentData getFileContentData() {
		return getFileContentData(100_000);
	}

	public FileContentData getFileContentData(int maxContentLength) {
		if (contentData == null) {
			if (contentParser != null) {
				contentData = contentParser.getFileContentData(maxContentLength);
			} else if (contentDataSupplier != null) {
				contentData = contentDataSupplier.get();
			}
		}
		return contentData;
	}

	public String getDetectedLanguage() {
		if (getFileContentData() != null) {
			if (contentData.getLanguage() == null && contentParser != null) {
				contentParser.getContentLanguage();
			}
			return contentData.getLanguage();
		} else {
			return null;
		}
	}

	public MimeType getMimeTypeData() {
		MimeType mimeType = MimeType.getMimeType(fileName);
		if (mimeType == null && contentData != null) {
			mimeType = MimeType.getMimeTypeByMime(contentData.getMimeType());
		}
		return mimeType;
	}

	public List<FullTextIndexValue> getFullTextIndexData(int maxContentLength) {
		FileContentData contentData = getFileContentData(maxContentLength);
		StringBuilder sb = new StringBuilder(getFileName());
		if (contentData.getContent() != null) {
			sb.append(" ").append(contentData.getContent());
		}
		return Collections.singletonList(new FullTextIndexValue("content", sb.toString()));
	}

	public String getMimeType() {
		return getFileContentData() == null ? null : getFileContentData().getMimeType();
	}

	public String getTextContent() {
		return getFileContentData() == null ? null : getFileContentData().getContent();
	}

	public String getLatitude() {
		return getFileContentData() == null ? null : getFileContentData().getLatitude();
	}

	public String getLongitude() {
		return getFileContentData() == null ? null : getFileContentData().getLongitude();
	}

	public int getPageCount() {
		return getFileContentData() == null ? 0 : getFileContentData().getPages();
	}

	public int getImageWidth() {
		return getFileContentData() == null ? 0 : getFileContentData().getImageWidth();
	}

	public int getImageHeight() {
		return getFileContentData() == null ? 0 : getFileContentData().getImageHeight();
	}

	public String getContentCreatedBy() {
		return getFileContentData() == null ? null : getFileContentData().getCreatedBy();
	}

	public Instant getContentCreationDate() {
		return getFileContentData() == null ? null : getFileContentData().getDateCreated();
	}

	public String getContentModifiedBy() {
		return getFileContentData() == null ? null : getFileContentData().getModifiedBy();
	}

	public Instant getContentModificationDate() {
		return getFileContentData() == null ? null : getFileContentData().getDateModified();
	}

	public String getDevice() {
		return getFileContentData() == null ? null : getFileContentData().getDevice();
	}

	public String getSoftware() {
		return getFileContentData() == null ? null : getFileContentData().getSoftware();
	}


}
