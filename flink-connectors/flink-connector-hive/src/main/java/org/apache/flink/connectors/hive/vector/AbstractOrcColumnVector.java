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

package org.apache.flink.connectors.hive.vector;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

/**
 * This column vector is used to adapt hive's ColumnVector to Flink's ColumnVector.
 */
public abstract class AbstractOrcColumnVector implements
		org.apache.flink.table.dataformat.vector.ColumnVector {

	private ColumnVector vector;

	AbstractOrcColumnVector(ColumnVector vector) {
		this.vector = vector;
	}

	@Override
	public boolean isNullAt(int i) {
		return !vector.noNulls && vector.isNull[i];
	}

	@Override
	public void reset() {
		throw new UnsupportedOperationException();
	}

	public static AbstractOrcColumnVector createVector(ColumnVector vector) {
		if (vector instanceof LongColumnVector) {
			return new OrcLongColumnVector((LongColumnVector) vector);
		} else if (vector instanceof DoubleColumnVector) {
			return new OrcDoubleColumnVector((DoubleColumnVector) vector);
		} else if (vector instanceof BytesColumnVector) {
			return new OrcBytesColumnVector((BytesColumnVector) vector);
		} else if (vector instanceof DecimalColumnVector) {
			return new OrcDecimalColumnVector((DecimalColumnVector) vector);
		} else {
			throw new UnsupportedOperationException("Unsupport vector: " + vector.getClass().getName());
		}
	}
}
