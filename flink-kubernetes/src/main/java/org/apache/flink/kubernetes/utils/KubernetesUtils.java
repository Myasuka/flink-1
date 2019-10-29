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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_MAP_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.FLINK_CONF_VOLUME;

/**
 * Common utils for Kubernetes.
 */
public class KubernetesUtils {

	/**
	 * Read file content to string.
	 * @param filePath file path
	 * @return content
	 */
	@Nullable
	public static String getContentFromFile(String filePath) {
		File file = new File(filePath);
		if (file.exists()) {
			StringBuilder content = new StringBuilder();
			String line;
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))){
				while ((line = reader.readLine()) != null) {
					content.append(line).append(System.lineSeparator());
				}
			} catch (IOException e) {
				throw new RuntimeException("Error read file content.", e);
			}
			return content.toString();
		}
		return null;
	}

	/**
	 * Generates the shell command to start a job manager for kubernetes.
	 * @param flinkConfig The Flink configuration.
	 * @param jobManagerMemoryMb JobManager heap size.
	 * @param configDirectory The configuration directory for the flink-conf.yaml
	 * @param logDirectory The log directory.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @param mainClass The main class to start with.
	 * @param mainArgs The args for main class.
	 * @return A String containing the job manager startup command.
	 */
	public static String getJobManagerStartCommand(
		Configuration flinkConfig,
		int jobManagerMemoryMb,
		String configDirectory,
		String logDirectory,
		boolean hasLogback,
		boolean hasLog4j,
		String mainClass,
		@Nullable String mainArgs) {

		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		startCommandValues.put("classpath", "-classpath " + "$" + Constants.ENV_FLINK_CLASSPATH);

		startCommandValues.put("jvmmem", String.format("-Xms%dm -Xmx%dm", jobManagerMemoryMb, jobManagerMemoryMb));

		startCommandValues.put("jvmopts", getJavaOpts(flinkConfig, CoreOptions.FLINK_JM_JVM_OPTIONS));

		startCommandValues.put("logging",
			getLogging(logDirectory + "/jobmanager.log", configDirectory, hasLogback, hasLog4j));

		startCommandValues.put("class", mainClass);

		startCommandValues.put("args", mainArgs != null ? mainArgs : "");

		startCommandValues.put("redirects",
			"1> " + logDirectory + "/jobmanager.out " +
			"2> " + logDirectory + "/jobmanager.err");

		final String commandTemplate = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE);
		return BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
	}

	/**
	 * Generates the shell command to start a task manager for kubernetes.
	 * @param flinkConfig The Flink configuration.
	 * @param tmParams Parameters for the task manager.
	 * @param configDirectory The configuration directory for the flink-conf.yaml
	 * @param logDirectory The log directory.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @param mainClass The main class to start with.
	 * @param mainArgs The args for main class.
	 * @return A String containing the task manager startup command.
	 */
	public static String getTaskManagerShellCommand(
		Configuration flinkConfig,
		ContaineredTaskManagerParameters tmParams,
		String configDirectory,
		String logDirectory,
		boolean hasLogback,
		boolean hasLog4j,
		Class<?> mainClass,
		String mainArgs) {

		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		startCommandValues.put("classpath", "-classpath " + "$" + Constants.ENV_FLINK_CLASSPATH);

		ArrayList<String> params = new ArrayList<>();
		params.add(String.format("-Xms%dm", tmParams.taskManagerHeapSizeMB()));
		params.add(String.format("-Xmx%dm", tmParams.taskManagerHeapSizeMB()));

		if (tmParams.taskManagerDirectMemoryLimitMB() >= 0) {
			params.add(String.format("-XX:MaxDirectMemorySize=%dm",
				tmParams.taskManagerDirectMemoryLimitMB()));
		}

		startCommandValues.put("jvmmem", StringUtils.join(params, ' '));

		startCommandValues.put("jvmopts", getJavaOpts(flinkConfig, CoreOptions.FLINK_TM_JVM_OPTIONS));

		startCommandValues.put("logging",
			getLogging(logDirectory + "/taskmanager.log", configDirectory, hasLogback, hasLog4j));

		startCommandValues.put("class", mainClass.getName());

		startCommandValues.put("args", mainArgs != null ? mainArgs : "");

		startCommandValues.put("redirects",
			"1> " + logDirectory + "/taskmanager.out " +
			"2> " + logDirectory + "/taskmanager.err");

		final String commandTemplate = flinkConfig
			.getString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE);
		return BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
	}

	/**
	 * Get config map volume for job manager and task manager pod.
	 * @param clusterId Cluster id.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @return Config map volume.
	 */
	public static Volume getConfigMapVolume(
		String clusterId,
		boolean hasLogback,
		boolean hasLog4j) {

		Volume configMapVolume = new Volume();
		configMapVolume.setName(FLINK_CONF_VOLUME);
		List<KeyToPath> items = new ArrayList<>();
		items.add(new KeyToPath(FLINK_CONF_FILENAME, null, FLINK_CONF_FILENAME));

		if (hasLogback) {
			items.add(new KeyToPath(CONFIG_FILE_LOGBACK_NAME, null, CONFIG_FILE_LOGBACK_NAME));
		}

		if (hasLog4j) {
			items.add(new KeyToPath(CONFIG_FILE_LOG4J_NAME, null, CONFIG_FILE_LOG4J_NAME));
		}
		configMapVolume.setConfigMap(new ConfigMapVolumeSourceBuilder()
			.withName(CONFIG_MAP_PREFIX + clusterId)
			.withItems(items)
			.build());
		return configMapVolume;
	}

	/**
	 * Get config map volume for job manager and task manager pod.
	 * @param flinkConfDirInPod Flink conf directory that will be mounted in the pod.
	 * @param hasLogback Uses logback?
	 * @param hasLog4j Uses log4j?
	 * @return Volume mount list.
	 */
	public static List<VolumeMount> getConfigMapVolumeMount(
		String flinkConfDirInPod,
		boolean hasLogback,
		boolean hasLog4j) {

		List<VolumeMount> volumeMounts = new ArrayList<>();
		volumeMounts.add(new VolumeMountBuilder()
			.withName(FLINK_CONF_VOLUME)
			.withMountPath(new File(flinkConfDirInPod, FLINK_CONF_FILENAME).getPath())
			.withSubPath(FLINK_CONF_FILENAME).build());

		if (hasLogback) {
			volumeMounts.add(new VolumeMountBuilder()
				.withName(FLINK_CONF_VOLUME)
				.withMountPath(new File(flinkConfDirInPod, CONFIG_FILE_LOGBACK_NAME).getPath())
				.withSubPath(CONFIG_FILE_LOGBACK_NAME)
				.build());
		}

		if (hasLog4j) {
			volumeMounts.add(new VolumeMountBuilder()
				.withName(FLINK_CONF_VOLUME)
				.withMountPath(new File(flinkConfDirInPod, CONFIG_FILE_LOG4J_NAME).getPath())
				.withSubPath(CONFIG_FILE_LOG4J_NAME)
				.build());
		}
		return volumeMounts;
	}

	/**
	 * Get resource requirements from memory and cpu.
	 * @param mem Memory in mb.
	 * @param cpu cpu.
	 * @return Resource requirements.
	 */
	public static ResourceRequirements getResourceRequirements(int mem, double cpu) {

		Quantity cpuQuantity = new Quantity(String.valueOf(cpu));
		Quantity memQuantity = new Quantity(String.valueOf(mem + Constants.RESOURCE_UNIT_MB));

		return new ResourceRequirementsBuilder()
			.addToRequests(Constants.RESOURCE_NAME_MEMORY, memQuantity)
			.addToRequests(Constants.RESOURCE_NAME_CPU, cpuQuantity)
			.addToLimits(Constants.RESOURCE_NAME_MEMORY, memQuantity)
			.addToLimits(Constants.RESOURCE_NAME_CPU, cpuQuantity)
			.build();
	}

	private static String getJavaOpts(Configuration flinkConfig, ConfigOption<String> configOption) {
		String javaOpts = flinkConfig.getString(CoreOptions.FLINK_JVM_OPTIONS);
		if (flinkConfig.getString(configOption).length() > 0) {
			javaOpts += " " + flinkConfig.getString(configOption);
		}
		return javaOpts;
	}

	private static String getLogging(String logFile, String confDir, boolean hasLogback, boolean hasLog4j) {
		StringBuilder logging = new StringBuilder();
		if (hasLogback || hasLog4j) {
			logging.append("-Dlog.file=").append(logFile);
			if (hasLogback) {
				logging.append(" -Dlogback.configurationFile=file:").append(confDir).append("/logback.xml");
			}
			if (hasLog4j) {
				logging.append(" -Dlog4j.configuration=file:").append(confDir).append("/log4j.properties");
			}
		}
		return logging.toString();
	}
}