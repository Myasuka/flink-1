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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.filesystem.FileSystemUtils.searchPartSpecAndPaths;
import static org.apache.flink.table.filesystem.TempFileManager.deleteCheckpoint;
import static org.apache.flink.table.filesystem.TempFileManager.headCheckpoints;
import static org.apache.flink.table.filesystem.TempFileManager.listTaskTemporaryPaths;

/**
 * File system file committer implementation. It move all files to output path from temporary path.
 *
 * <p>In a checkpoint:
 *  1.Every task will invoke {@link #createFileManagerAndCleanDir} to initialization, it returns
 *  a path generator to generate path for task writing. And clean the temporary path of task.
 *  2.After writing done for this checkpoint, need invoke {@link #commitUpToCheckpoint(long)},
 *  will move the temporary files to real output path.
 *
 * <p>Batch is a special case of Streaming, which has only one checkpoint.
 *
 * <p>Data consistency:
 * 1.For task failure: will launch a new task and invoke {@link #createFileManagerAndCleanDir},
 *   this will clean previous temporary files (This simple design can make it easy to delete the
 *   invalid temporary directory of the task, but it also causes that our directory does not
 *   support the same task to start multiple backups to run).
 * 2.For job master commit failure when overwrite: this may result in unfinished intermediate
 *   results, but if we try to run job again, the final result must be correct (because the
 *   intermediate result will be overwritten).
 * 3.For job master commit failure when append: This can lead to inconsistent data. But,
 *   considering that the commit action is a single point of execution, and only moves files and
 *   updates metadata, it will be faster, so the probability of inconsistency is relatively small.
 *
 * <p>See:
 * {@link TempFileManager}.
 * {@link FileSystemLoader}.
 */
@Internal
public class FileSystemCommitter implements Serializable {

	private static final long serialVersionUID = 1L;

	private final FileSystemFactory factory;
	private final MetaStoreFactory metaStoreFactory;
	private final boolean overwrite;
	private final Path tmpPath;
	private final LinkedHashMap<String, String> staticPartitions;
	private final int partitionColumnSize;

	public FileSystemCommitter(
			FileSystemFactory factory,
			MetaStoreFactory metaStoreFactory,
			boolean overwrite,
			Path tmpPath,
			LinkedHashMap<String, String> staticPartitions,
			int partitionColumnSize) {
		this.factory = factory;
		this.metaStoreFactory = metaStoreFactory;
		this.overwrite = overwrite;
		this.tmpPath = tmpPath;
		this.staticPartitions = staticPartitions;
		this.partitionColumnSize = partitionColumnSize;
	}

	/**
	 * For committing job's output after successful batch job completion or one checkpoint finish
	 * for streaming job. Should move all files to final output paths.
	 *
	 * <p>NOTE: According to checkpoint notify mechanism of Flink, checkpoint may fail and be
	 * abandoned, so this method should commit all checkpoint ids that less than current
	 * checkpoint id (Includes failure checkpoints).
	 */
	public void commitUpToCheckpoint(long toCpId) throws Exception {
		FileSystem fs = factory.create(tmpPath.toUri());

		try (FileSystemLoader loader = new FileSystemLoader(overwrite, fs, metaStoreFactory)) {
			for (long cp : headCheckpoints(fs, tmpPath, toCpId)) {
				commitSingleCheckpoint(fs, loader, cp);
			}
		}
	}

	private void commitSingleCheckpoint(
			FileSystem fs, FileSystemLoader loader, long checkpointId) throws Exception {
		try {
			List<Path> taskPaths = listTaskTemporaryPaths(fs, tmpPath, checkpointId);
			if (partitionColumnSize > 0) {
				for (Map.Entry<LinkedHashMap<String, String>, List<Path>> entry :
						collectPartSpecToPaths(fs, taskPaths).entrySet()) {
					loader.loadPartition(entry.getKey(), entry.getValue());
				}
			} else {
				loader.loadNonPartition(taskPaths);
			}
		} finally {
			deleteCheckpoint(fs, tmpPath, checkpointId);
		}
	}

	/**
	 * Create a new temporary file manager from task and checkpoint id.
	 * And clean the temporary directory for task.
	 */
	public TempFileManager createFileManagerAndCleanDir(
			int taskNumber, long checkpointId) throws Exception {
		TempFileManager pathGenerator = new TempFileManager(
				tmpPath, taskNumber, checkpointId);
		Path path = pathGenerator.getTaskTemporaryPath();
		factory.create(path.toUri()).delete(path, true);
		return pathGenerator;
	}

	/**
	 * Collect all partitioned paths, aggregate according to partition spec.
	 */
	private Map<LinkedHashMap<String, String>, List<Path>> collectPartSpecToPaths(
			FileSystem fs, List<Path> taskPaths) throws IOException {
		boolean isDynamicPartition = partitionColumnSize != staticPartitions.size();
		Map<LinkedHashMap<String, String>, List<Path>> specToPaths = new HashMap<>();
		if (isDynamicPartition) {
			for (Path taskPath : taskPaths) {
				searchPartSpecAndPaths(fs, taskPath, partitionColumnSize).forEach(
						tuple2 -> specToPaths.compute(tuple2.f0, (spec, paths) -> {
							paths = paths == null ? new ArrayList<>() : paths;
							paths.add(tuple2.f1);
							return paths;
						}));
			}
		} else {
			specToPaths.put(staticPartitions, taskPaths);
		}
		return specToPaths;
	}
}
