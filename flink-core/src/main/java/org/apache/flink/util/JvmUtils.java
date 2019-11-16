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

package org.apache.flink.util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

/**
 * Utilities for {@link java.lang.management.ManagementFactory}.
 */
public class JvmUtils {

	/**
	 * Returns the thread info for all live threads with stack trace and synchronization information.
	 *
	 * @return the thread dump info of current JVM
	 */
	public static String threadDumpInfo(){

		ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
		ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);

		return Arrays
			.stream(threadInfos)
			.sorted(Comparator.comparing(ThreadInfo::getThreadId))
			.map(Object::toString)
			.collect(Collectors.joining());
	}

}
