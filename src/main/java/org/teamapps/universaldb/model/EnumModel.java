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
package org.teamapps.universaldb.model;

import org.teamapps.message.protocol.utils.MessageUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EnumModel {
	private static final int ENUM_MODEL_VERSION = 1;
	private final String name;
	private String title;

	private List<String> enumNames;
	private List<String> enumTitles;

	private final boolean remoteEnum;
	private final String remoteDatabase;
	private final String remoteDatabaseNamespace;


	private boolean deprecated;
	private boolean deleted;
	private int dateCreated;
	private int dateModified;
	private int versionCreated;
	private int versionModified;

	protected EnumModel(String name, String title, List<String> enumNames, List<String> enumTitles) {
		this(name, title, enumNames, enumTitles, false, null, null);
	}

	protected EnumModel(String name, String title, List<String> enumNames, List<String> enumTitles, boolean remoteEnum, String  remoteDatabase, String remoteDatabaseNamespace) {
		this.name = NamingUtils.createName(name);
		this.title = NamingUtils.createTitle(title);
		this.enumNames = enumNames.stream().map(NamingUtils::createName).collect(Collectors.toList());
		this.enumTitles = enumTitles.stream().map(NamingUtils::createTitle).collect(Collectors.toList());
		NamingUtils.checkName(name, title);
		this.remoteEnum = remoteEnum;
		this.remoteDatabase = remoteDatabase;
		this.remoteDatabaseNamespace = remoteDatabaseNamespace;
	}

	protected EnumModel(DataInputStream dis) throws IOException {
		//int modelVersion = dis.readInt(); //todo
		name = MessageUtils.readString(dis);
		title = MessageUtils.readString(dis);
		enumNames = MessageUtils.readStringList(dis);
		enumTitles = MessageUtils.readStringList(dis);
		remoteEnum = false; //dis.readBoolean(); //todo
		remoteDatabase = remoteEnum ? MessageUtils.readString(dis) : null;
		remoteDatabaseNamespace = remoteEnum ? MessageUtils.readString(dis) : null;
		deprecated = dis.readBoolean();
		deleted = dis.readBoolean();
		dateCreated = dis.readInt();
		dateModified = dis.readInt();
		versionCreated = dis.readInt();
		versionModified = dis.readInt();
	}

	public void write(DataOutputStream dos) throws IOException {
		//dos.writeInt(ENUM_MODEL_VERSION); //todo
		MessageUtils.writeString(dos, name);
		MessageUtils.writeString(dos, title);
		MessageUtils.writeStringList(dos, enumNames);
		MessageUtils.writeStringList(dos, enumTitles);
		//dos.writeBoolean(remoteEnum); //todo
		if (remoteEnum) {
			MessageUtils.writeString(dos, remoteDatabase);
			MessageUtils.writeString(dos, remoteDatabaseNamespace);
		}
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

	public boolean isRemoteEnum() {
		return remoteEnum;
	}

	public String getRemoteDatabase() {
		return remoteDatabase;
	}

	public String getRemoteDatabaseNamespace() {
		return remoteDatabaseNamespace;
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
