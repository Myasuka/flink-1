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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Dynamic partition writer to writing multiple partitions at the same time, it maybe consumes more memory.
 */
@Internal
public class DynamicPartitionWriter<T> implements PartitionWriter<T> {

	private final OutputFormatFactory<T> factory;

	private Map<String, OutputFormat<T>> formats;
	private Context<T> context;

	public DynamicPartitionWriter(OutputFormatFactory<T> factory) {
		this.factory = factory;
	}

	@Override
	public void open(Context<T> context) throws Exception {
		this.context = context;
		this.formats = new HashMap<>();
	}

	@Override
	public void startTransaction() throws Exception {
		this.formats.clear();
	}

	@Override
	public void write(T in) throws Exception {
		String partition = context.computePartition(in);
		OutputFormat<T> format = formats.get(partition);

		if (format == null) {
			// create a new format to write new partition.
			format = factory.createOutputFormat(context.generatePath(partition));
			context.prepareOutputFormat(format);
			formats.put(partition, format);
		}
		format.writeRecord(context.projectColumnsToWrite(in));
	}

	@Override
	public void endTransaction() throws Exception {
		for (OutputFormat<?> format : formats.values()) {
			format.close();
		}
	}
}
