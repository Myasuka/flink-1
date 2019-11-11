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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Internal
class ProcessingTimeServiceImpl implements ProcessingTimeService {
	private final TimerService timerService;

	private final Function<ProcessingTimeCallback, ProcessingTimeCallback> processingTimeCallbackWrapper;

	private final ConcurrentHashMap<ScheduledFuture<?>, Object> pendingTimers;

	private final CompletableFuture<?> timersDoneFutureAfterQuiescing;

	private volatile boolean isQuiesced;

	ProcessingTimeServiceImpl(
			TimerService timerService,
			Function<ProcessingTimeCallback, ProcessingTimeCallback> processingTimeCallbackWrapper) {
		this.timerService = timerService;
		this.processingTimeCallbackWrapper = processingTimeCallbackWrapper;

		this.pendingTimers = new ConcurrentHashMap<>();
		this.timersDoneFutureAfterQuiescing = new CompletableFuture<>();
		this.isQuiesced = false;
	}

	@Override
	public long getCurrentProcessingTime() {
		return timerService.getCurrentProcessingTime();
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
		if (isQuiesced) {
			return new NeverCompleteFuture(
				ProcessingTimeServiceUtil.getProcessingTimeDelay(timestamp, getCurrentProcessingTime()));
		}

		final TimerScheduledFuture<?> timer = new TimerScheduledFuture<>();
		timer.setTimer(timerService.registerTimer(timestamp, invokeCallbackAndRemovePending(timer, target)));

		pendingTimers.put(timer, true);
		if (timer.isDone()) {
			removePendingTimers(timer);
		}
		return timer;
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		return timerService.scheduleAtFixedRate(processingTimeCallbackWrapper.apply(callback), initialDelay, period);
	}

	void quiesce() {
		isQuiesced = true;
		completeTimersDoneFutureIfQuiesced();
	}

	CompletableFuture<?> getTimersDoneFutureAfterQuiescing() {
		return timersDoneFutureAfterQuiescing;
	}

	@VisibleForTesting
	int getNumPendingTimers() {
		return pendingTimers.size();
	}

	private ProcessingTimeCallback invokeCallbackAndRemovePending(TimerScheduledFuture<?> timer, ProcessingTimeCallback target) {
		ProcessingTimeCallback originalCallback = processingTimeCallbackWrapper.apply(target);

		return timestamp -> {
			try {
				originalCallback.onProcessingTime(timestamp);
			} finally {
				removePendingTimers(timer);
			}
		};
	}

	private void removePendingTimers(TimerScheduledFuture<?> timer) {
		pendingTimers.remove(timer);
		completeTimersDoneFutureIfQuiesced();
	}

	private void completeTimersDoneFutureIfQuiesced() {
		if (isQuiesced && pendingTimers.size() == 0) {
			timersDoneFutureAfterQuiescing.complete(null);
		}
	}

	private class TimerScheduledFuture<V> implements ScheduledFuture<V> {

		private ScheduledFuture<?> timer;

		void setTimer(ScheduledFuture<?> timer) {
			this.timer = timer;
		}

		@Override
		public long getDelay(@Nonnull TimeUnit unit) {
			return timer.getDelay(unit);
		}

		@Override
		public int compareTo(@Nonnull Delayed o) {
			return timer.compareTo(o);
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			boolean canceled = timer.cancel(mayInterruptIfRunning);
			if (canceled) {
				removePendingTimers(this);
			}
			return canceled;
		}

		@Override
		public boolean isCancelled() {
			return timer.isCancelled();
		}

		@Override
		public boolean isDone() {
			return timer.isDone();
		}

		@SuppressWarnings("unchecked")
		@Override
		public V get() throws InterruptedException, ExecutionException {
			return (V) timer.get();
		}

		@SuppressWarnings("unchecked")
		@Override
		public V get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return (V) timer.get(timeout, unit);
		}
	}
}
