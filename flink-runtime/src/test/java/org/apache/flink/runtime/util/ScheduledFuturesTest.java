package org.apache.flink.runtime.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Assert;
import org.junit.Test;

public class ScheduledFuturesTest {

	private static final int TIMEOUT_MS = 100;

	// ------------------------------------------------------------------------
	// Test ExecutorService/ScheduledExecutorService behaviour.
	// ------------------------------------------------------------------------

	@Test
	public void testExecutorService_uncaughtExceptionHandler() throws InterruptedException {
		final CountDownLatch handlerCalled = new CountDownLatch(1);
		final ThreadFactory threadFactory = new ThreadFactoryBuilder()
			.setDaemon(true)
			.setUncaughtExceptionHandler((t, e) -> handlerCalled.countDown())
			.build();
		final ExecutorService scheduledExecutorService =
			Executors.newSingleThreadExecutor(threadFactory);
		scheduledExecutorService.execute(() -> {
			throw new RuntimeException("foo");
		});
		Assert.assertTrue(
			"Expected handler to be called.",
			handlerCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testScheduledExecutorService_uncaughtExceptionHandler() throws InterruptedException {
		final CountDownLatch handlerCalled = new CountDownLatch(1);
		final ThreadFactory threadFactory = new ThreadFactoryBuilder()
			.setDaemon(true)
			.setUncaughtExceptionHandler((t, e) -> handlerCalled.countDown())
			.build();
		final ScheduledExecutorService scheduledExecutorService =
			Executors.newSingleThreadScheduledExecutor(threadFactory);
		scheduledExecutorService.execute(() -> {
			throw new RuntimeException("foo");
		});
		Assert.assertFalse(
			"Expected handler not to be called.",
			handlerCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
	}

	// ------------------------------------------------------------------------
	// Test ScheduledFutures.
	// ------------------------------------------------------------------------

	@Test
	public void testWithUncaughtExceptionHandler_runtimeException() throws InterruptedException {
		final RuntimeException expected = new RuntimeException("foo");
		testWithUncaughtExceptionHandler(() -> {
			throw expected;
		}, expected);
	}

	@Test
	public void testWithUncaughtExceptionHandler_error() throws InterruptedException {
		final Error expected = new Error("foo");
		testWithUncaughtExceptionHandler(() -> {
			throw expected;
		}, expected);
	}

	private static void testWithUncaughtExceptionHandler(Runnable runnable, Throwable expected)
		throws InterruptedException {
		ScheduledExecutorService scheduledExecutorService =
			Executors.newSingleThreadScheduledExecutor();
		final AtomicReference<Throwable> reference = new AtomicReference<>();
		final CountDownLatch handlerCalled = new CountDownLatch(1);
		ScheduledFutures.withUncaughtExceptionHandler(
			scheduledExecutorService.schedule(runnable, 0, TimeUnit.MILLISECONDS), (t, e) -> {
				reference.set(e);
				handlerCalled.countDown();
			});
		Assert.assertTrue(handlerCalled.await(100, TimeUnit.MILLISECONDS));
		Assert.assertNotNull(reference.get());
		Assert.assertEquals(expected.getClass(), reference.get().getClass());
		Assert.assertEquals("foo", reference.get().getMessage());
	}
}
