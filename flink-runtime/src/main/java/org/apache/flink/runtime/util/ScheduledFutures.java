package org.apache.flink.runtime.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Utils related to {@link ScheduledFuture scheduled futures}.
 */
public class ScheduledFutures {

	private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
		.setNameFormat("Flink-Scheduled-Future-SafeGuard")
		.setDaemon(true)
		.build();

	/**
	 * Guard {@link ScheduledFuture} with scheduled future uncaughtException handler, because
	 * {@link java.util.concurrent.ScheduledExecutorService} does not respect the one assigned to
	 * executing {@link Thread} instance.
	 *
	 * @param scheduledFuture Scheduled future to guard.
	 * @param uncaughtExceptionHandler Handler to call in case of uncaught exception.
	 * @param <T> Type the future returns.
	 * @return Future with handler.
	 */
	public static <T> ScheduledFuture<T> withUncaughtExceptionHandler(
		ScheduledFuture<T> scheduledFuture,
		Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
		final Thread safeguardThread = THREAD_FACTORY.newThread(() -> {
			try {
				scheduledFuture.get();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e.getCause());
			}
		});
		safeguardThread.start();
		return scheduledFuture;
	}

	/**
	 * Guard {@link ScheduledFuture} with scheduled future {@link FatalExitExceptionHandler}
	 * uncaughtException handler.
	 * 
	 * @param scheduledFuture Scheduled future to guard.
	 * @param <T> Type the future returns.
	 * @return Future with handler.
	 *
	 * @see #withUncaughtExceptionHandler(ScheduledFuture, Thread.UncaughtExceptionHandler)
	 */
	public static <T> ScheduledFuture<T> withFatalExitUncaughtException(
		ScheduledFuture<T> scheduledFuture) {
		return withUncaughtExceptionHandler(scheduledFuture, FatalExitExceptionHandler.INSTANCE);
	}
}
