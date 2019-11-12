/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.resources;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ResourceValue}.
 */
public class ResourceValueTest extends TestLogger {

	@Test
	public void testConstructor_valid() {
		final ResourceValue v1 = new TestResourceValue(0.1);
		assertValueEquals(0.1, v1);

		final ResourceValue v2 = new TestResourceValue(0.1, 0.01);
		assertValueEquals(0.1, v2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConstructor_invalidValue() {
		new TestResourceValue(-0.1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConstructor_invalidPrecision() {
		new TestResourceValue(0.1, -0.01);
	}

	@Test
	public void testCompare_strictMatches() {
		final ResourceValue v1 = new TestResourceValue(0.1, 0.0);
		final ResourceValue v2 = new TestResourceValue(0.2, 0.0);
		assertTrue(v1.compareTo(v2) < 0);
		assertTrue(v2.compareTo(v1) > 0);

		final ResourceValue v3 = new TestResourceValue(0.1, 0.0);
		assertTrue(v1.compareTo(v3) == 0);
	}

	@Test
	public void testCompare_withPrecision() {
		final ResourceValue v1 = new TestResourceValue(0.1, 0.01);
		final ResourceValue v2 = new TestResourceValue(0.109, 0.01);
		assertTrue(v1.compareTo(v2) == 0);

		final ResourceValue v3 = new TestResourceValue(0.111, 0.01);
		assertTrue(v1.compareTo(v3) < 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCompare_errorOnDifferentPrecision() {
		final ResourceValue v1 = new TestResourceValue(0.1, 0.01);
		final ResourceValue v2 = new TestResourceValue(0.1, 0.02);
		v1.compareTo(v2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCompare_errorOnDifferentType() {
		final ResourceValue v1 = new TestResourceValue(0.1);
		final ResourceValue v2 = new AdditiveResourceValue(0.1);
		v1.compareTo(v2);
	}

	public static void assertValueEquals(final double expectedValue, final ResourceValue actualValue) {
		assertEquals(expectedValue, actualValue.getValue(), actualValue.getPrecision());
	}
}
