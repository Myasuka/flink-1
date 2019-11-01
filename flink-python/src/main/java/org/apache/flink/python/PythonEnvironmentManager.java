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

import org.apache.beam.model.pipeline.v1.RunnerApi;

import java.io.IOException;

/**
 * The base interface of python environment manager which is used to create the Environment object
 * and the RetrievalToken of Beam Fn API.
 */
@Internal
public interface PythonEnvironmentManager {

	/**
	 * Initialize the environment manager.
	 */
	void open();

	/**
	 * Creates the Environment object of Apache Beam Fn API.
	 *
	 * @return Environment object represent the environment(process, docker, etc)
	 *         the python worker would run on.
	 */
	RunnerApi.Environment createEnvironment();

	/**
	 * Creates the RetrievalToken of Apache Beam Fn API. It contains a list of files which need to
	 * transmit through ArtifactService provided by Apache Beam.
	 *
	 * @return The path of the RetrievalToken file.
	 */
	String createRetrievalToken() throws IOException;

	/**
	 * Remove the temporary files generated during the lifetime of this object.
	 */
	void close();
}
