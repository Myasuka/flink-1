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

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Value of a resource. The underlying value is a double with an acceptable precision.
 * The value can be compared via {@link #compareTo(ResourceValue)} which ignores small deltas.
 */
public abstract class ResourceValue implements Serializable, Comparable<ResourceValue> {

	private static final long serialVersionUID = 1L;

	private static final double DEFAULT_PRECISION = 1e-15;

	protected final double value;

	protected final double precision;

	public ResourceValue(final double value) {
		this(value, DEFAULT_PRECISION);
	}

	public ResourceValue(final double value, final double precision) {
		checkArgument(precision >= 0.0, "Precision must no be negative");

		this.value = value;
		this.precision = precision;

		if (!isValid()) {
			throw new IllegalArgumentException(String.format("Resource value {} is not valid.", value));
		}
	}

	private boolean isValid() {
		return value > 0.0 || isZero();
	}

	private boolean equalsWithPrecision(final double otherValue) {
		return Math.abs(value - otherValue) <= precision;
	}

	public boolean isZero() {
		return equalsWithPrecision(0.0);
	}

	public double getValue() {
		return value;
	}

	public double getPrecision() {
		return precision;
	}

	public abstract ResourceValue merge(final ResourceValue other);

	public abstract ResourceValue subtract(final ResourceValue other);

	/**
	 * Compare with another resource value.
	 * Note: this class has a natural ordering that is inconsistent with equals.
	 * It ignores smaller deltas within the given precision.
	 *
	 * @param other resource value to compare
	 * @return 0 if value equals, positive if this value is larger, positive if
	 *         negative the other value is larger
	 * @throws NullPointerException if the specified object is null
	 * @throws IllegalArgumentException if the other resource value has a different
	 *         precision or if it is a different implementation
	 */
	@Override
	public int compareTo(final ResourceValue other) {
		checkNotNull(other, "Cannot compare with null resources");

		if (this == other) {
			return 0;
		}

		checkArgument(
			precision == other.precision,
			"Cannot compare resource values with different precisions");
		checkArgument(
			getClass() == other.getClass(),
			"Cannot compare resource values of different types");

		if (equalsWithPrecision(other.value)) {
			return 0;
		} else if (value > other.value) {
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	public boolean equals(final Object other) {
		if (this == other) {
			return true;
		} else if (other != null && getClass() == other.getClass()) {
			ResourceValue that = (ResourceValue) other;

			return value == that.value && precision == that.precision;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = Double.hashCode(value);
		result = 31 * result + Double.hashCode(precision);
		return result;
	}

	@Override
	public String toString() {
		return value + " +/- " + precision;
	}
}
