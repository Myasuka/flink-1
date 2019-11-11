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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ProcessingTimeServiceImpl}.
 */
public class ProcessingTimeServiceImplTest extends TestLogger {

	private static final Time testingTimeout = Time.seconds(10L);

	private SystemProcessingTimeService timerService;
	private CompletableFuture<Throwable> errorFuture;

	@Before
	public void setup() throws Exception {
		errorFuture = new CompletableFuture<>();
		timerService = new SystemProcessingTimeService(errorFuture::complete);
	}

	@After
	public void teardown() {
		timerService.shutdownService();
	}

	@Test
	public void testTimerRegistrationAndCancellation() {
		ProcessingTimeServiceImpl processingTimeService = new ProcessingTimeServiceImpl(timerService, v -> v);

		ScheduledFuture<?> timer = processingTimeService.registerTimer(Long.MAX_VALUE, timestamp -> {});
		assertEquals(1, processingTimeService.getNumPendingTimers());
		assertEquals(1, timerService.getNumTasksScheduled());

		timer.cancel(false);
		assertEquals(0, processingTimeService.getNumPendingTimers());
		assertEquals(0, timerService.getNumTasksScheduled());
	}

	@Test
	public void testRegisteringTimerThatThrowsException() throws InterruptedException, ExecutionException, TimeoutException {
		ProcessingTimeServiceImpl processingTimeService = new ProcessingTimeServiceImpl(timerService, v -> v);

		ScheduledFuture<?> timer = processingTimeService.registerTimer(
			processingTimeService.getCurrentProcessingTime() - 5L, timestamp -> {
				throw new Exception("Test exception.");
			});

		timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertTrue(errorFuture.isDone());
		assertEquals(0, processingTimeService.getNumPendingTimers());
		assertEquals(0, timerService.getNumTasksScheduled());
	}

	@Test
	public void testQuiescingAndTimersDoneFuture() throws Exception {
		ProcessingTimeServiceImpl processingTimeService = new ProcessingTimeServiceImpl(timerService, v -> v);

		final OneShotLatch latch = new OneShotLatch();

		ScheduledFuture<?> timer = processingTimeService.registerTimer(
			processingTimeService.getCurrentProcessingTime() - 5L,
			timestamp -> latch.await());

		processingTimeService.quiesce();
		assertEquals(1, processingTimeService.getNumPendingTimers());

		// should be able to schedule more tasks but never get executed
		ScheduledFuture<?> neverExecutedTimer = processingTimeService.registerTimer(Long.MAX_VALUE, timestamp -> {});
		assertNotNull(neverExecutedTimer);
		assertEquals(1, processingTimeService.getNumPendingTimers());

		// after the timer is completed, the "timers-done" future should become "done"
		assertFalse(processingTimeService.getTimersDoneFutureAfterQuiescing().isDone());
		latch.trigger();
		timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertTrue(processingTimeService.getTimersDoneFutureAfterQuiescing().isDone());
		assertEquals(0, processingTimeService.getNumPendingTimers());
		assertEquals(0, timerService.getNumTasksScheduled());
	}
}
