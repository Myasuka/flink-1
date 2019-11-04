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
 * Tests for {@link AdditiveResourceValue}.
 */
public class AdditiveResourceValueTest extends TestLogger {

	@Test
	public void testMerge() {
		final ResourceValue v1 = new AdditiveResourceValue(0.1);
		final ResourceValue v2 = new AdditiveResourceValue(0.2);
		assertEquals(0.3, v1.merge(v2).getValue(), v1.getPrecision());
	}

	@Test
	public void testMerge_onOverflow() {
		final ResourceValue v1 = new AdditiveResourceValue(Double.MAX_VALUE);
		final ResourceValue v2 = new AdditiveResourceValue(0.1);
		assertTrue(Double.compare(Double.MAX_VALUE, v1.merge(v2).getValue()) == 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMerge_errorOnDifferentPrecision() {
		final ResourceValue v1 = new AdditiveResourceValue(0.1, 0.01);
		final ResourceValue v2 = new AdditiveResourceValue(0.1, 0.02);
		v1.merge(v2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMerge_errorOnDifferentType() {
		final ResourceValue v1 = new AdditiveResourceValue(0.1);
		final ResourceValue v2 = new TestResourceValue(0.1);
		v1.merge(v2);
	}

	@Test
	public void testSubtract() {
		final ResourceValue v1 = new AdditiveResourceValue(0.2);
		final ResourceValue v2 = new AdditiveResourceValue(0.1);
		assertEquals(0.1, v1.subtract(v2).getValue(), v1.getPrecision());
	}

	@Test
	public void testSubtract_fromInf() {
		final ResourceValue v1 = new AdditiveResourceValue(Double.MAX_VALUE);
		final ResourceValue v2 = new AdditiveResourceValue(0.1);
		assertTrue(Double.compare(Double.MAX_VALUE, v1.subtract(v2).getValue()) == 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSubtract_largerValue() {
		final ResourceValue v1 = new AdditiveResourceValue(0.1);
		final ResourceValue v2 = new AdditiveResourceValue(0.2);
		v1.subtract(v2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSubtract_errorOnDifferentPrecision() {
		final ResourceValue v1 = new AdditiveResourceValue(0.1, 0.01);
		final ResourceValue v2 = new AdditiveResourceValue(0.1, 0.02);
		v1.subtract(v2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSubtract_errorOnDifferentType() {
		final ResourceValue v1 = new AdditiveResourceValue(0.1);
		final ResourceValue v2 = new TestResourceValue(0.1);
		v1.subtract(v2);
	}
}
