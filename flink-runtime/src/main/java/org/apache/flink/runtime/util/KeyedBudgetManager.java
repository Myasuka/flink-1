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

package org.apache.flink.runtime.util;

import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Manages {@code long} available budget per key (allocation/release).
 *
 * <p>This manager gets a certain maximum {@code long} budget per key.
 * Users can allocate some budget for some key and release it later.
 * The manager keeps track of allocated/released budget and prevents from over-allocating.
 *
 * <p>There is also a paged type of allocation where a certain number of pages can be allocated from a set of keys.
 * The page has its budget size. The manager allocates randomly from all keys of a given set.
 * At the end, sum of pages allocated from each key is either requested number of pages or none.
 * Only integer number of pages are allocated from each key respecting its available budget (no page spans two or more keys)
 * or nothing is allocated reporting the maximum number of pages which could be allocated per each given key at the moment.
 *
 * @param <K> type of the budget key
 */
@ThreadSafe
public class KeyedBudgetManager<K> {
	private final Map<K, Long> maxBudgetByKey;

	private final long defaultPageSize;

	private final long totalNumberOfPages;

	@GuardedBy("lock")
	private final Map<K, Long> availableBudgetByKey;

	private final Object lock = new Object();

	public KeyedBudgetManager(Map<K, Long> maxBudgetByKey, long defaultPageSize) {
		Preconditions.checkNotNull(maxBudgetByKey);
		Preconditions.checkArgument(defaultPageSize > 0L, "The default page size has to be greater than zero");

		this.maxBudgetByKey = new HashMap<>(maxBudgetByKey);
		this.availableBudgetByKey = new HashMap<>(maxBudgetByKey);
		this.defaultPageSize = defaultPageSize;
		this.totalNumberOfPages = calculateTotalNumberOfPages(maxBudgetByKey, defaultPageSize);
	}

	public long getDefaultPageSize() {
		return defaultPageSize;
	}

	/**
	 * Tries to acquire budget which equals to the number of pages times default page size.
	 *
	 * <p>See also {@link #acquirePagedBudgetForKeys(Iterable, long, long)}
	 */
	public Either<Map<K, Long>, Long> acquirePagedBudget(long numberOfPages) {
		return acquirePagedBudgetForKeys(maxBudgetByKey.keySet(), numberOfPages, defaultPageSize);
	}

	/**
	 * Tries to acquire budget for a given key.
	 *
	 * <p>No budget is acquired if it was not possible to fully acquire the requested budget.
	 *
	 * @param key the key to acquire budget from
	 * @param size the size of budget to acquire from the given key
	 * @return the fully acquired budget for the key or max possible budget to acquire
	 * if it was not possible to acquire the requested budget.
	 */
	public long acquireBudgetForKey(K key, long size) {
		Preconditions.checkNotNull(key);
		Either<Map<K, Long>, Long> result = acquirePagedBudgetForKeys(Collections.singletonList(key), size, 1L);
		return result.isLeft() ? result.left().get(key) : result.right();
	}

	/**
	 * Tries to acquire budget which equals to the number of pages times page size.
	 *
	 * <p>The budget will be acquired only from the given keys. Only integer number of pages will be acquired from each key.
	 * If the next page does not fit into the available budget of some key, it will try to be acquired from another key.
	 * The acquisition is successful if the acquired number of pages for each key sums up to the requested number of pages.
	 * The function does not make any preference about which keys from the given keys to acquire from.
	 *
	 * @param keys the keys to acquire budget from
	 * @param numberOfPages the total number of pages to acquire from the given keys
	 * @param pageSize the size of budget to acquire per page
	 * @return the acquired number of pages for each key if the acquisition is successful (either left) or
	 * the total number of pages which were available for the given keys (either right).
	 */
	Either<Map<K, Long>, Long> acquirePagedBudgetForKeys(Iterable<K> keys, long numberOfPages, long pageSize) {
		Preconditions.checkNotNull(keys);
		Preconditions.checkArgument(numberOfPages >= 0L, "The requested number of pages has to be positive");
		Preconditions.checkArgument(pageSize > 0L, "The page size has to be greater than zero");

		synchronized (lock) {
			long totalPossiblePages = 0L;
			Map<K, Long> pagesToReserveByKey = new HashMap<>();
			for (K key : keys) {
				long currentKeyBudget = availableBudgetByKey.getOrDefault(key, 0L);
				long currentKeyPages = currentKeyBudget / pageSize;
				if (totalPossiblePages + currentKeyPages >= numberOfPages) {
					pagesToReserveByKey.put(key, numberOfPages - totalPossiblePages);
					totalPossiblePages = numberOfPages;
					break;
				} else if (currentKeyPages > 0L) {
					pagesToReserveByKey.put(key, currentKeyPages);
					totalPossiblePages += currentKeyPages;
				}
			}
			boolean possibleToAcquire = totalPossiblePages == numberOfPages;
			if (possibleToAcquire) {
				for (Entry<K, Long> pagesToReserveForKey : pagesToReserveByKey.entrySet()) {
					//noinspection ConstantConditions
					availableBudgetByKey.compute(
						pagesToReserveForKey.getKey(),
						(k, v) -> v - (pagesToReserveForKey.getValue() * pageSize));
				}
			}
			return possibleToAcquire ? Either.Left(pagesToReserveByKey) : Either.Right(totalPossiblePages);
		}
	}

	public void releasePageForKey(K key) {
		releaseBudgetForKey(key, defaultPageSize);
	}

	public void releaseBudgetForKey(K key, long size) {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(size >= 0L, "The budget to release has to be positive");

		releaseBudgetForKeys(Collections.singletonMap(key, size));
	}

	public void releaseBudgetForKeys(Map<K, Long> sizeByKey) {
		Preconditions.checkNotNull(sizeByKey);

		synchronized (lock) {
			for (Entry<K, Long> toReleaseForKey : sizeByKey.entrySet()) {
				long toRelease = toReleaseForKey.getValue();
				Preconditions.checkArgument(
					toRelease >= 0L,
					"The budget to release for key %s has to be positive",
					toReleaseForKey.getKey());
				if (toRelease == 0L) {
					continue;
				}
				K keyToReleaseFor = toReleaseForKey.getKey();
				long maxBudgetForKey = maxBudgetByKey.get(keyToReleaseFor);
				availableBudgetByKey.compute(keyToReleaseFor, (k, currentBudget) -> {
					if (currentBudget == null) {
						throw new IllegalArgumentException("The budget key is not supported: " + keyToReleaseFor);
					} else if (currentBudget + toRelease > maxBudgetForKey) {
						throw new IllegalStateException(
							String.format(
								"The budget to release %d exceeds the limit %d for key %s",
								toRelease,
								maxBudgetForKey,
								keyToReleaseFor));
					} else {
						return currentBudget + toRelease;
					}
				});
			}
		}
	}

	public void releaseAll() {
		synchronized (lock) {
			availableBudgetByKey.putAll(maxBudgetByKey);
		}
	}

	public long maxTotalBudget() {
		return maxBudgetByKey.values().stream().mapToLong(b -> b).sum();
	}

	public long maxTotalNumberOfPages() {
		return totalNumberOfPages;
	}

	public long maxTotalBudgetForKey(K key) {
		Preconditions.checkNotNull(key);
		return maxBudgetByKey.get(key);
	}

	public long totalAvailableBudget() {
		return availableBudgetForKeys(maxBudgetByKey.keySet());
	}

	long availableBudgetForKeys(Iterable<K> keys) {
		Preconditions.checkNotNull(keys);
		synchronized (lock) {
			long totalSize = 0L;
			for (K key : keys) {
				totalSize += availableBudgetForKey(key);
			}
			return totalSize;
		}
	}

	long availableBudgetForKey(K key) {
		Preconditions.checkNotNull(key);
		synchronized (lock) {
			return availableBudgetByKey.getOrDefault(key, 0L);
		}
	}

	private static <K> long calculateTotalNumberOfPages(Map<K, Long> budgetByType, long pageSize) {
		long numPages = 0L;
		for (long sizeForType : budgetByType.values()) {
			numPages += sizeForType / pageSize;
		}
		return numPages;
	}
}
