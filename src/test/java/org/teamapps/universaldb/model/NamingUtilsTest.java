package org.teamapps.universaldb.model;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class NamingUtilsTest {

	@Test
	public void createTitle() {
		Assert.assertEquals("This is the title", NamingUtils.createTitle("thisIsTheTitle"));
		Assert.assertEquals("This is the title", NamingUtils.createTitle("THIS_IS_THE_TITLE"));
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