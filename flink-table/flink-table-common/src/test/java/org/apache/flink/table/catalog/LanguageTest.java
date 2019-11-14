package org.apache.flink.table.catalog;

import org.junit.Assert;
import org.junit.Test;

public class LanguageTest {

	@Test
	public void testConversion() {
		Assert.assertEquals(Language.valueOf("java"), Language.JAVA);
	}
}
