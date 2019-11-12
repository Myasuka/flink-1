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

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of {@link ResourceValue} which merges data by addition and doing the inverse by subtraction.
 */
public class AdditiveResourceValue extends ResourceValue {

	private static final long serialVersionUID = 1L;

	public AdditiveResourceValue(final double value) {
		super(value);
	}

	public AdditiveResourceValue(final double value, final double delta) {
		super(value, delta);
	}

	public ResourceValue merge(final ResourceValue other) {
		checkNotNull(other, "Cannot merge with null resources");
		checkArgument(
			precision == other.precision,
			"Cannot merge resource values with different precisions");
		checkArgument(
			getClass() == other.getClass(),
			"Cannot merge resource values of different types");

		double result = value + other.value;

		if (result == Double.POSITIVE_INFINITY) {
			result = Double.MAX_VALUE;
		}

		return new AdditiveResourceValue(result, precision);
	}

	public ResourceValue subtract(final ResourceValue other) {
		checkNotNull(other, "Cannot subtract with null resources");
		checkArgument(
			precision == other.precision,
			"Cannot subtract resource values with different precisions");
		checkArgument(
			getClass() == other.getClass(),
			"Cannot subtract resource values of different types");
		checkArgument(
			compareTo(other) >= 0, "Try to subtract a larger resource value from this one.");

		final double result = value == Double.MAX_VALUE ? Double.MAX_VALUE : value - other.value;

		return new AdditiveResourceValue(result, precision);
	}
}
