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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.dataformat.vector.VectorizedColumnBatch.DEFAULT_SIZE;

/**
 * {@link HiveReader} to read files using flink orc reader.
 */
public class FlinkOrcReader implements HiveReader {

	private OrcColumnarRowReader reader;

	public FlinkOrcReader(
			JobConf jobConf,
			List<String> partitionKeys,
			String[] fieldNames,
			DataType[] fieldTypes,
			int[] selectedFields,
			HiveTableInputSplit split) throws IOException {
		StorageDescriptor sd = split.getHiveTablePartition().getStorageDescriptor();
		List<FieldSchema> cols = sd.getCols();
		List<TypeDescription> typeDescriptions = cols.stream()
				.map(FieldSchema::getType)
				.map(TypeInfoUtils::getTypeInfoFromTypeString)
				.map(OrcInputFormat::convertTypeInfo)
				.collect(Collectors.toList());
		TypeDescription typeDescription = TypeDescription.createStruct();
		for (int i = 0; i < typeDescriptions.size(); i++) {
			typeDescription.addField(cols.get(i).getName(), typeDescriptions.get(i));
		}

		Configuration conf = new Configuration(jobConf);
		sd.getSerdeInfo().getParameters().forEach(conf::set);

		InputSplit hadoopSplit = split.getHadoopInputSplit();
		FileSplit fileSplit;
		if (hadoopSplit instanceof FileSplit) {
			fileSplit = (FileSplit) hadoopSplit;
		} else {
			throw new IllegalArgumentException("Unknown split type: " + hadoopSplit);
		}

		this.reader = new OrcColumnarRowReader(
				conf,
				typeDescription,
				selectedFields,
				fieldNames,
				fieldTypes,
				partitionKeys,
				split.getHiveTablePartition().getPartitionSpec(),
				new ArrayList<>(),
				DEFAULT_SIZE,
				new Path(fileSplit.getPath().toString()),
				fileSplit.getStart(),
				fileSplit.getLength());
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.reader.reachedEnd();
	}

	@Override
	public BaseRow nextRecord(BaseRow reuse) throws IOException {
		return this.reader.nextRecord(reuse);
	}

	@Override
	public void close() throws IOException {
		this.reader.close();
	}
}
