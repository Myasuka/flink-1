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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.connectors.hive.vector.AbstractOrcColumnVector;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcReader;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.dataformat.vector.heap.AbstractHeapVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBooleanVector;
import org.apache.flink.table.dataformat.vector.heap.HeapByteVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBytesVector;
import org.apache.flink.table.dataformat.vector.heap.HeapDoubleVector;
import org.apache.flink.table.dataformat.vector.heap.HeapFloatVector;
import org.apache.flink.table.dataformat.vector.heap.HeapIntVector;
import org.apache.flink.table.dataformat.vector.heap.HeapLongVector;
import org.apache.flink.table.dataformat.vector.heap.HeapShortVector;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.functions.SqlDateTimeUtils.dateToInternal;
import static org.apache.flink.table.runtime.functions.SqlDateTimeUtils.timeToInternal;
import static org.apache.flink.table.runtime.functions.SqlDateTimeUtils.timestampToInternal;

/**
 * {@link OrcReader} to read ORC files into {@link BaseRow}.
 */
public class OrcColumnarRowReader extends OrcReader<BaseRow> {

	// the vector of rows that is read in a batch
	private final VectorizedColumnBatch columnarBatch;

	private final ColumnarRow row;

	public OrcColumnarRowReader(
			Configuration conf,
			TypeDescription schema,
			int[] selectedFields,
			String[] fieldNames,
			DataType[] fieldTypes,
			List<String> partitionKeys,
			Map<String, Object> partitionSpec,
			List<Predicate> conjunctPredicates,
			int batchSize,
			Path path,
			long splitStart,
			long splitLength) throws IOException {
		super(conf, schema, selectedFields, conjunctPredicates, batchSize, path, splitStart, splitLength);
		// create and initialize the row batch
		ColumnVector[] vectors = new ColumnVector[selectedFields.length];
		for (int i = 0; i < vectors.length; i++) {
			int index = selectedFields[i];
			String name = fieldNames[index];
			if (partitionKeys.contains(name)) {
				DataType type = fieldTypes[index];
				AbstractHeapVector heapVector = AbstractHeapVector.createHeapColumn(
						type.getLogicalType(), batchSize);
				fill(heapVector, batchSize, type.getLogicalType(), partitionSpec.get(name));
				vectors[i] = heapVector;
			} else {
				vectors[i] = AbstractOrcColumnVector.createVector(rowBatch.cols[index]);
			}
		}
		this.columnarBatch = new VectorizedColumnBatch(vectors);
		this.row = new ColumnarRow(columnarBatch);
	}

	@Override
	protected int fillRows() {
		int size = rowBatch.size;
		columnarBatch.setNumRows(size);
		return size;
	}

	@Override
	public BaseRow nextRecord(BaseRow reuse) {
		// return the next row
		row.setRowId(this.nextRow++);
		return row;
	}

	private static void fill(AbstractHeapVector vector, int size, LogicalType fieldType, Object value) {
		if (value == null) {
			for (int i = 0; i < size; i++) {
				vector.setNullAt(i);
			}
			return;
		}

		switch (fieldType.getTypeRoot()) {
			case BOOLEAN:
				Arrays.fill(((HeapBooleanVector) vector).vector, (Boolean) value);
				return;
			case TINYINT:
				Arrays.fill(((HeapByteVector) vector).vector, (Byte) value);
				return;
			case DOUBLE:
				Arrays.fill(((HeapDoubleVector) vector).vector, (Double) value);
				return;
			case FLOAT:
				Arrays.fill(((HeapFloatVector) vector).vector, (Float) value);
				return;
			case INTEGER:
				Arrays.fill(((HeapIntVector) vector).vector, (Integer) value);
				return;
			case DATE:
				Arrays.fill(((HeapIntVector) vector).vector, dateToInternal((Date) value));
				return;
			case TIME_WITHOUT_TIME_ZONE:
				Arrays.fill(((HeapIntVector) vector).vector, timeToInternal((Time) value));
				return;
			case BIGINT:
				Arrays.fill(((HeapLongVector) vector).vector, (Long) value);
				return;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				Arrays.fill(((HeapLongVector) vector).vector, timestampToInternal((Timestamp) value));
				return;
			case SMALLINT:
				Arrays.fill(((HeapShortVector) vector).vector, (Short) value);
				return;
			case DECIMAL:
				DecimalType decimalType = (DecimalType) fieldType;
				Decimal decimal = Decimal.fromBigDecimal(
						(BigDecimal) value, decimalType.getPrecision(), decimalType.getScale());
				if (decimal == null) {
					throw new RuntimeException("Decimal conversion fail: " + value);
				}

				if (Decimal.is32BitDecimal(decimalType.getPrecision())) {
					Arrays.fill(((HeapIntVector) vector).vector, (int) decimal.toUnscaledLong());
					return;
				} else if (Decimal.is64BitDecimal(decimalType.getPrecision())) {
					Arrays.fill(((HeapLongVector) vector).vector, decimal.toUnscaledLong());
					return;
				} else {
					byte[] bytes = decimal.toUnscaledBytes();
					fillBytes((HeapBytesVector) vector, bytes);
					return;
				}
			case CHAR:
			case VARCHAR:
				byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
				fillBytes((HeapBytesVector) vector, bytes);
				return;
			case BINARY:
			case VARBINARY:
				fillBytes((HeapBytesVector) vector, (byte[]) value);
				return;
			default:
				throw new UnsupportedOperationException(fieldType  + " is not supported now.");
		}
	}

	private static void fillBytes(HeapBytesVector vector, byte[] bytes) {
		vector.buffer = new byte[vector.start.length * bytes.length];
		for (int i = 0; i < vector.start.length; i++) {
			int start = bytes.length * i;
			System.arraycopy(bytes, 0, vector.buffer, start, bytes.length);
			vector.start[i] = start;
			vector.length[i] = bytes.length;
		}
	}
}
