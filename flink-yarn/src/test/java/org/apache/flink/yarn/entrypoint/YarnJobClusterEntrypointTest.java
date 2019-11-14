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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link YarnJobClusterEntrypoint}.
 */
public class YarnJobClusterEntrypointTest {

	@Test
	public void testCreateDispatcherResourceManagerComponentFactoryFailIfUsrLibDirDoesNotExist(){
		final Configuration configuration = new Configuration();
		configuration.setString(YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR, YarnConfigOptions.UserJarInclusion.DISABLED.toString());
		final YarnJobClusterEntrypoint yarnJobClusterEntrypoint = new YarnJobClusterEntrypoint(configuration, FileUtils.getCurrentWorkingDirectory().getClass().toString());
		try {
			yarnJobClusterEntrypoint.createDispatcherResourceManagerComponentFactory(configuration);
			fail();
		} catch (Throwable exception) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(exception, "the usrlib directory does not exist.").isPresent());
		}
	}
}
