/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.fs.Path;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests parsing python dependency metadata.
 */
public class PythonDependencyManagerTest {

	private DistributedCache distributedCache;

	@Before
	public void prepareDistributeCache() {
		Map<String, Future<Path>> distributeCachedFiles = new HashMap<>();
		distributeCachedFiles.put("python_file_0_{uuid}",
			wrapFuture(new Path("/distributed_cache/file0")));
		distributeCachedFiles.put("python_file_1_{uuid}",
			wrapFuture(new Path("/distributed_cache/file1")));
		distributeCachedFiles.put("python_requirements_file_{uuid}",
			wrapFuture(new Path("/distributed_cache/file2")));
		distributeCachedFiles.put("python_requirements_cache_{uuid}",
			wrapFuture(new Path("/distributed_cache/file3")));
		distributeCachedFiles.put("python_archive_0_{uuid}",
			wrapFuture(new Path("/distributed_cache/file4")));
		distributeCachedFiles.put("python_archive_1_{uuid}",
			wrapFuture(new Path("/distributed_cache/file5")));
		distributedCache = new DistributedCache(distributeCachedFiles);
	}

	@Test
	public void testParsePythonFiles() throws IOException {
		Map<String, String> pythonFileMetaData = new HashMap<>();
		pythonFileMetaData.put(PythonDependencyManager.PYTHON_FILE_MAP,
			"{\"python_file_0_{uuid}\": \"test_file1.py\", \"python_file_1_{uuid}\": \"test_file2.py\"}");
		PythonDependencyManager dependencyManager =
			PythonDependencyManager.create(pythonFileMetaData, distributedCache);

		Map<String, String> expected = new HashMap<>();
		expected.put("/distributed_cache/file0", "test_file1.py");
		expected.put("/distributed_cache/file1", "test_file2.py");
		assertEquals(expected, dependencyManager.getFilesInPythonPath());
	}

	@Test
	public void testParsePythonRequirements() throws IOException {
		Map<String, String> pythonFileMetaData = new HashMap<>();
		pythonFileMetaData.put(PythonDependencyManager.PYTHON_REQUIREMENTS_FILE, "python_requirements_file_{uuid}");
		PythonDependencyManager dependencyManager =
			PythonDependencyManager.create(pythonFileMetaData, distributedCache);

		assertEquals("/distributed_cache/file2", dependencyManager.getRequirementsFilePath().get());
		assertFalse(dependencyManager.getRequirementsCacheDir().isPresent());

		pythonFileMetaData.put(PythonDependencyManager.PYTHON_REQUIREMENTS_CACHE, "python_requirements_cache_{uuid}");
		dependencyManager = PythonDependencyManager.create(pythonFileMetaData, distributedCache);

		assertEquals("/distributed_cache/file2", dependencyManager.getRequirementsFilePath().get());
		assertEquals("/distributed_cache/file3", dependencyManager.getRequirementsCacheDir().get());
	}

	@Test
	public void testParsePythonArchives() throws IOException {
		Map<String, String> pythonFileMetaData = new HashMap<>();
		pythonFileMetaData.put(PythonDependencyManager.PYTHON_ARCHIVES_MAP,
			"{\"python_archive_0_{uuid}\": \"py27.zip\", \"python_archive_1_{uuid}\": \"py37\"}");
		PythonDependencyManager dependencyManager =
			PythonDependencyManager.create(pythonFileMetaData, distributedCache);

		Map<String, String> expected = new HashMap<>();
		expected.put("/distributed_cache/file4", "py27.zip");
		expected.put("/distributed_cache/file5", "py37");
		assertEquals(expected, dependencyManager.getArchives());
	}

	@Test
	public void testParsePythonExec() throws IOException {
		Map<String, String> pythonFileMetaData = new HashMap<>();
		pythonFileMetaData.put(PythonDependencyManager.PYTHON_EXEC, "/usr/bin/python3");
		PythonDependencyManager dependencyManager =
			PythonDependencyManager.create(pythonFileMetaData, distributedCache);

		assertEquals("/usr/bin/python3", dependencyManager.getPythonExec().get());
	}

	private <T> Future<T> wrapFuture(T element) {
		return new Future<T>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				return false;
			}

			@Override
			public boolean isCancelled() {
				return false;
			}

			@Override
			public boolean isDone() {
				return false;
			}

			@Override
			public T get() throws InterruptedException, ExecutionException {
				return element;
			}

			@Override
			public T get(long timeout, @Nonnull TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
				return element;
			}
		};
	}
}
