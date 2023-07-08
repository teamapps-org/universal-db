/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class NamingUtilsTest {

	@Test
	public void createTitle() {
		Assert.assertEquals("This is the title", NamingUtils.createTitle("thisIsTheTitle"));
		Assert.assertEquals("This is the title", NamingUtils.createTitle("THIS_IS_THE_TITLE"));

		assertEquals("User id", NamingUtils.createTitle("userID"));
	}

	@Test
	public void createName() {
		Assert.assertEquals("thisIsTheTitle", NamingUtils.createName("This is the title"));
		Assert.assertEquals("thisIsTheTitle", NamingUtils.createName("THIS_IS_THE_TITLE"));
	}

	@Test
	public void tokenize() {
	}

	@Test
	public void createConstantName() {
		Assert.assertEquals("THIS_IS_THE_TITLE", NamingUtils.createConstantName("This is the title"));
	}

	@Test
	public void createTitleFromCamelCase() {
		Assert.assertEquals("This is the title", NamingUtils.createTitleFromCamelCase("thisIsTheTitle"));
	}
}
