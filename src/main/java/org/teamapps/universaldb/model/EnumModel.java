package org.teamapps.universaldb.model;

import org.teamapps.message.protocol.utils.MessageUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EnumModel {
	private final String name;
	private String title;

	private List<String> enumNames;
	private List<String> enumTitles;
	private boolean deprecated;
	private boolean deleted;
	private int dateCreated;
	private int dateModified;
	private int versionCreated;
	private int versionModified;


	protected EnumModel(String title, List<String> enumTitles) {
		this(title, title, enumTitles, enumTitles);
	}

	protected EnumModel(String name, String title, List<String> enumTitles) {
		this(name, title, enumTitles, enumTitles);
	}

	protected EnumModel(String name, String title, List<String> enumNames, List<String> enumTitles) {
		this.name = NamingUtils.createName(name);
		this.title = NamingUtils.createTitle(title);
		this.enumNames = enumNames.stream().map(NamingUtils::createName).collect(Collectors.toList());
		this.enumTitles = enumTitles.stream().map(NamingUtils::createConstantName).collect(Collectors.toList());
	}

	protected EnumModel(DataInputStream dis) throws IOException {
		name = MessageUtils.readString(dis);
		title = MessageUtils.readString(dis);
		enumNames = MessageUtils.readStringList(dis);
		enumTitles = MessageUtils.readStringList(dis);
		deprecated = dis.readBoolean();
		deleted = dis.readBoolean();
		dateCreated = dis.readInt();
		dateModified = dis.readInt();
		versionCreated = dis.readInt();
		versionModified = dis.readInt();
	}

	public void write(DataOutputStream dos) throws IOException {
		MessageUtils.writeString(dos, name);
		MessageUtils.writeString(dos, title);
		MessageUtils.writeStringList(dos, enumNames);
		MessageUtils.writeStringList(dos, enumTitles);
		dos.writeBoolean(deprecated);
		dos.writeBoolean(deleted);
		dos.writeInt(dateCreated);
		dos.writeInt(dateModified);
		dos.writeInt(versionCreated);
		dos.writeInt(versionModified);
	}

	public String getName() {
		return name;
	}

	public String getTitle() {
		return title;
	}

	protected void setTitle(String title) {
		this.title = title;
	}

	public List<String> getEnumNames() {
		return new ArrayList<>(enumNames);
	}

	public List<String> getEnumTitles() {
		return new ArrayList<>(enumTitles);
	}

	protected void updateValues(List<String> enumNames, List<String> enumTitles) {
		this.enumNames = enumNames;
		this.enumTitles = enumTitles;
	}

	public boolean isDeprecated() {
		return deprecated;
	}

	protected void setDeprecated(boolean deprecated) {
		this.deprecated = deprecated;
	}

	public boolean isDeleted() {
		return deleted;
	}

	protected void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public int getDateCreated() {
		return dateCreated;
	}

	protected void setDateCreated(int dateCreated) {
		this.dateCreated = dateCreated;
	}

	public int getDateModified() {
		return dateModified;
	}

	protected void setDateModified(int dateModified) {
		this.dateModified = dateModified;
	}

	public int getVersionCreated() {
		return versionCreated;
	}

	protected void setVersionCreated(int versionCreated) {
		this.versionCreated = versionCreated;
	}

	public int getVersionModified() {
		return versionModified;
	}

	protected void setVersionModified(int versionModified) {
		this.versionModified = versionModified;
	}

}
