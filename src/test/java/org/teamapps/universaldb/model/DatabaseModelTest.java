package org.teamapps.universaldb.model;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class DatabaseModelTest {

	@Test
	public void mergeModel() {
	}

	@Test
	public void isCompatible() {
		DatabaseModel model = createModel();
		DatabaseModel model3 = createModel();
		TableModel table = model3.createTable("test");
		table.addTimestamp("test");

		Assert.assertTrue(model.isCompatible(model3));
		TableModel table2 = model.createTable("test");
		Assert.assertTrue(model.isCompatible(model3));
		table2.addTime("test");
		Assert.assertFalse(model.isCompatible(model3));
	}

	@Test
	public void isValid() {
		DatabaseModel model = createModel();
		Assert.assertTrue(model.isValid());
	}

	@Test
	public void checkErrors() {
	}

	@Test
	public void checkIds() {
	}

	@Test
	public void isSameModel() {
		DatabaseModel model = createModel();
		DatabaseModel model2 = createModel();
		DatabaseModel model3 = createModel();
		model3.createTable("test");

		Assert.assertTrue(model.isSameModel(model2));
		Assert.assertFalse(model.isSameModel(model3));
		model.initialize();
		Assert.assertTrue(model.isSameModel(model2));
	}

	@Test
	public void toBytes() throws IOException {
		DatabaseModel model = createModel();
		byte[] bytes = model.toBytes();
		DatabaseModel model2 = new DatabaseModel(bytes);
		Assert.assertTrue(model.isSameModel(model2));
	}

	private static DatabaseModel createModel() {
		DatabaseModel model = new DatabaseModel("model", "Model", "org.teamapps.model");
		EnumModel enumModel = model.createEnum("enum", "valOne", "valTwo");
		TableModel table = model.createTable("table1", "Table 1");
		table.addText("field1");
		table.addTimestamp("field2");


		return model;
	}
}