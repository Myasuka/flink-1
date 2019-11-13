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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeComparator;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;

import java.time.LocalDateTime;

/**
 * {@link TypeInformation} for {@link LocalDateTime}.
 *
 * <p>The difference between Types.LOCAL_DATE_TIME is this TypeInformation holds a precision
 */
public class LegacyLocalDateTimeTypeInfo extends LocalTimeTypeInfo<LocalDateTime> {

	private final int precision;

	public LegacyLocalDateTimeTypeInfo(int precision) {
		super(
			LocalDateTime.class,
			LocalDateTimeSerializer.INSTANCE,
			LocalDateTimeComparator.class);
		this.precision = precision;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof LegacyLocalDateTimeTypeInfo)) {
			return false;
		}
		LegacyLocalDateTimeTypeInfo that = (LegacyLocalDateTimeTypeInfo) obj;
		return this.precision == that.precision;
	}

	@Override
	public String toString() {
		return String.format("Timestamp(%d)", precision);
	}

	public int getPrecision() {
		return precision;
	}
}