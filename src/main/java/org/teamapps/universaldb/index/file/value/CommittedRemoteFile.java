package org.teamapps.universaldb.index.file.value;

import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.FileValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Supplier;

public class CommittedRemoteFile implements FileValue {

	private final Supplier<File> fileSupplier;
	private final String fileName;
	private final String hash;
	private final long size;
	private final Supplier<FileContentData> contentDataSupplier;
	private File file;
	private FileContentData contentData;

	public CommittedRemoteFile(Supplier<File> fileSupplier, String fileName, String hash, long size, Supplier<FileContentData> contentDataSupplier) {
		this.fileSupplier = fileSupplier;
		this.fileName = fileName;
		this.hash = hash;
		this.size = size;
		this.contentDataSupplier = contentDataSupplier;
	}

	private File getFile() {
		if (file == null) {
			file = fileSupplier.get();
		}
		return file;
	}

	@Override
	public FileValueType getType() {
		return FileValueType.COMMITTED_REMOTE_FILE;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return new BufferedInputStream(new FileInputStream(getFile()));
	}

	@Override
	public File getAsFile() {
		try {
			Path path = Files.createTempFile("tmp", "." + getFileExtension());
			Files.copy(getFile().toPath(), path, StandardCopyOption.REPLACE_EXISTING);
			return path.toFile();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void copyToFile(File file) throws IOException {
		Files.copy(this.getFile().toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	@Override
	public String getFileName() {
		return fileName;
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public String getHash() {
		return hash;
	}

	@Override
	public String getKey() {
		return null;
	}

	@Override
	public FileContentData getFileContentData() {
		if (contentData == null) {
			contentData = contentDataSupplier.get();
		}
		return contentData;
	}

	@Override
	public String getDetectedLanguage() {
		return getFileContentData().getLanguage();
	}
}
