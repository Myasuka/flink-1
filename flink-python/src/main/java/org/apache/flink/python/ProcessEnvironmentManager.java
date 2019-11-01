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

package org.apache.flink.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.python.util.UnzipUtil;
import org.apache.flink.util.FileUtils;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The ProcessEnvironmentManager used to prepare the working dir of python UDF worker
 * and create ProcessEnvironment object of Beam Fn API. It will be created if the python
 * function runner is configured to run python UDF in process mode.
 */
@Internal
public final class ProcessEnvironmentManager implements PythonEnvironmentManager {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessEnvironmentManager.class);

	public static final String PYTHON_REQUIREMENTS_FILE = "_PYTHON_REQUIREMENTS_FILE";
	public static final String PYTHON_REQUIREMENTS_CACHE = "_PYTHON_REQUIREMENTS_CACHE";
	public static final String PYTHON_REQUIREMENTS_DIR_ENV = "_PYTHON_REQUIREMENTS_INSTALL_DIR";
	public static final String PYTHON_WORKING_DIR = "_PYTHON_WORKING_DIR";

	static final String PYTHON_TMP_DIR_PREFIX = "python_dist_";
	static final String PYTHON_REQUIREMENTS_DIR = "python_requirements_target";
	static final String PYTHON_ARCHIVES_DIR = "python_archives";
	static final String PYTHON_FILES_DIR = "python_files_dir";

	private PythonDependencyManager dependencyManager;
	private String basicDirectory;
	private String requirementsDirectory;
	private String archivesDirectory;
	private String filesDirectory;
	private Thread shutdownHook;
	private Map<String, String> systemEnv;

	private ProcessEnvironmentManager(
		PythonDependencyManager dependencyManager,
		String basicDirectory,
		String filesDirectory,
		String archivesDirectory,
		String requirementsDirectory,
		Map<String, String> systemEnv) {
		this.dependencyManager = dependencyManager;
		this.basicDirectory = basicDirectory;
		this.filesDirectory = filesDirectory;
		this.archivesDirectory = archivesDirectory;
		this.requirementsDirectory = requirementsDirectory;
		this.systemEnv = systemEnv;
	}

	@Override
	public void open() {
		shutdownHook = registerShutdownHook();
	}

	@Override
	public void close() {
		if (shutdownHook != null) {
			shutdownHook.run();
			Runtime.getRuntime().removeShutdownHook(shutdownHook);
			shutdownHook = null;
		}
	}

	@Override
	public RunnerApi.Environment createEnvironment() {
		prepareEnvironment();
		Map<String, String> generatedEnv = generateEnvironmentVariable();
		String flinkHomePath = systemEnv.get(ConfigConstants.ENV_FLINK_HOME_DIR);
		String pythonWorkerCommand = joinPath(flinkHomePath, "bin", "pyflink-udf-runner.sh");

		return Environments.createProcessEnvironment(
			"",
			"",
			pythonWorkerCommand,
			generatedEnv);
	}

	/**
	 * Just return a empty RetrievalToken because no files will be transmit via ArtifactService in process mode.
	 *
	 * @return The path of empty RetrievalToken.
	 */
	@Override
	public String createRetrievalToken() throws IOException {
		File retrievalToken = new File(basicDirectory,
			"retrieval_token_" + UUID.randomUUID().toString() + ".json");
		if (!retrievalToken.getParentFile().exists() && !retrievalToken.getParentFile().mkdir()) {
			throw new IOException(
				"Could not create the parent directory of RetrievalToken file: " +
					retrievalToken.getAbsolutePath());
		}
		if (retrievalToken.createNewFile()) {
			final DataOutputStream dos = new DataOutputStream(new FileOutputStream(retrievalToken));
			dos.writeBytes("{\"manifest\": {}}");
			dos.flush();
			dos.close();
			return retrievalToken.getAbsolutePath();
		} else {
			throw new IOException(
				"Could not create the RetrievalToken file: " + retrievalToken.getAbsolutePath());
		}
	}

	/**
	 * Generate the environment variables used to create the launcher process of python UDF worker.
	 *
	 * <p>To avoid unnecessary IO usage, when running in process mode no artifacts will be transmitted
	 * via ArtifactService of Beam. Instead, the path of artifacts will be transmit to the launcher
	 * of python UDF worker via environment variable.
	 *
	 * @return Environment variable map containing paths of python dependencies.
	 */
	@VisibleForTesting
	Map<String, String> generateEnvironmentVariable() {
		Map<String, String> systemEnv = new HashMap<>(this.systemEnv);

		// generate the PYTHONPATH of udf worker.
		if (!dependencyManager.getFilesInPythonPath().isEmpty()) {
			List<String> filesList = new ArrayList<>();
			for (Map.Entry<String, String> entry : dependencyManager.getFilesInPythonPath().entrySet()) {
				// The origin file name will be wiped during the transmission in Flink Distributed Cache
				// and replaced by a unreadable character sequence, now restore their origin name.
				// Every python file will be placed at :
				// ${basicDirectory}/python_files_dir/${distributedCacheFileName}/${originFileName}
				String distributedCacheFileName = new File(entry.getKey()).getName();
				String actualFileName = entry.getValue();
				File file = new File(entry.getKey());
				String pathForSearching;
				if (file.isFile() && actualFileName.endsWith(".py")) {
					// If the file is single py file, use its parent directory as PYTHONPATH.
					pathForSearching = joinPath(filesDirectory, distributedCacheFileName);
				} else {
					pathForSearching = joinPath(filesDirectory, distributedCacheFileName, actualFileName);
				}
				filesList.add(pathForSearching);
			}
			String pythonPath = String.join(File.pathSeparator, filesList);
			String systemPythonPath = systemEnv.get("PYTHONPATH");
			if (systemPythonPath != null && !systemPythonPath.isEmpty()) {
				pythonPath = String.join(File.pathSeparator, pythonPath, systemPythonPath);
			}
			if (!pythonPath.isEmpty()) {
				systemEnv.put("PYTHONPATH", pythonPath);
			}
			LOG.info("PYTHONPATH of python worker: {}", pythonPath);
		}

		// To support set python interpreter path in archives, the archives directory should be used as
		// working directory of udf worker.
		if (!dependencyManager.getArchives().isEmpty()) {
			systemEnv.put(PYTHON_WORKING_DIR, archivesDirectory);
			LOG.info("python working dir of python worker: {}", archivesDirectory);
		}

		// The requirements will be installed by a bootstrap script, here just transmit the necessary information
		// to the script via environment variable.
		if (dependencyManager.getRequirementsFilePath().isPresent()) {
			systemEnv.put(PYTHON_REQUIREMENTS_FILE, dependencyManager.getRequirementsFilePath().get());
			LOG.info("requirments.txt of python worker: {}", dependencyManager.getRequirementsFilePath().get());

			if (dependencyManager.getRequirementsCacheDir().isPresent()) {
				systemEnv.put(PYTHON_REQUIREMENTS_CACHE, dependencyManager.getRequirementsCacheDir().get());
				LOG.info("requirments cached dir of python worker: {}",
					dependencyManager.getRequirementsCacheDir().get());
			}

			systemEnv.put(PYTHON_REQUIREMENTS_DIR_ENV, requirementsDirectory);
			LOG.info("requirements install directory of python worker: {}", requirementsDirectory);
		}

		// Transmit the path of python interpreter to bootstrap script.
		if (dependencyManager.getPythonExec().isPresent()) {
			systemEnv.put("python", dependencyManager.getPythonExec().get());
			LOG.info("user defined python interpreter path: {}", dependencyManager.getPythonExec());
		}
		return systemEnv;
	}

	@VisibleForTesting
	void prepareEnvironment() {
		try {
			prepareWorkingDir();
		} catch (IllegalArgumentException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Prepare the working directory for running the python UDF worker.
	 *
	 * <p>For the files to append to PYTHONPATH directly, restore their origin file names.
	 * For the files used for pip installations, prepare the target directory to contain the install result.
	 * For the archive files, extract them to their target directory and try to restore their origin permissions.
	 *
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	private void prepareWorkingDir() throws IOException, IllegalArgumentException {
		for (Map.Entry<String, String> entry : dependencyManager.getFilesInPythonPath().entrySet()) {
			// The origin file name will be wiped during the transmission in Flink Distributed Cache
			// and replaced by a unreadable character sequence, now restore their origin name.
			// Every python file will be placed at :
			// ${basicDirectory}/python_files_dir/${distributedCacheFileName}/${originFileName}
			String distributedCacheFileName = new File(entry.getKey()).getName();
			String actualFileName = entry.getValue();
			Path target = FileSystems.getDefault().getPath(filesDirectory,
				distributedCacheFileName, actualFileName);
			if (!target.getParent().toFile().mkdirs()) {
				throw new IllegalArgumentException(
					String.format("Can not create the directory: %s !", target.getParent().toString()));
			}
			Path src = FileSystems.getDefault().getPath(entry.getKey());
			try {
				Files.createSymbolicLink(target, src);
			} catch (IOException e) {
				LOG.warn(String.format("Can not create the symbolic link of: %s, "
					+ "the link path is %s, fallback to copy.", src, target), e);
				FileUtils.copy(new org.apache.flink.core.fs.Path(src.toUri()),
					new org.apache.flink.core.fs.Path(target.toUri()), false);
			}
		}

		for (Map.Entry<String, String> entry : dependencyManager.getArchives().entrySet()) {
			UnzipUtil.extractZipFileWithPermissions(
				entry.getKey(), joinPath(archivesDirectory, entry.getValue()));
		}

		File requirementsDirectoryFile = new File(requirementsDirectory);
		if (!requirementsDirectoryFile.exists()) {
			if (!requirementsDirectoryFile.mkdirs()) {
				throw new IllegalArgumentException(
					String.format("Creating the requirements target directory: %s failed!",
						requirementsDirectory));
			}
		} else {
			if (!requirementsDirectoryFile.isDirectory()) {
				throw new IllegalArgumentException(
					String.format("The requirements target directory path: %s is not a directory!",
						requirementsDirectory));
			}
		}
	}

	private Thread registerShutdownHook() {
		Thread thread = new Thread(() -> FileUtils.deleteDirectoryQuietly(new File(basicDirectory)));
		Runtime.getRuntime().addShutdownHook(thread);
		return thread;
	}

	@VisibleForTesting
	String getBasicDirectory() {
		return basicDirectory;
	}

	static String joinPath(String... args) {
		return String.join(File.separator, args);
	}

	public static ProcessEnvironmentManager create(
		PythonDependencyManager dependencyManager,
		String tmpDirectoryRoot,
		Map<String, String> environmentVariable) {

		// The basic directory of all the generated files.
		String basicDirectory = new File(tmpDirectoryRoot,
			PYTHON_TMP_DIR_PREFIX + UUID.randomUUID().toString()).getAbsolutePath();
		// Archives directory is used to store the extract result of python archives.
		String archivesDirectory = joinPath(basicDirectory, PYTHON_ARCHIVES_DIR);
		// Requirements directory is used to store the install result of requirements file.
		String requirementsDirectory = joinPath(basicDirectory, PYTHON_REQUIREMENTS_DIR);
		// Files directory is used to store the python files or their symbolic links which will
		// be added to the PYTHONPATH of udf worker.
		String filesDirectory = joinPath(basicDirectory, PYTHON_FILES_DIR);

		return new ProcessEnvironmentManager(
			dependencyManager,
			basicDirectory,
			filesDirectory,
			archivesDirectory,
			requirementsDirectory,
			environmentVariable);
	}
}
