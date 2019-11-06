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

package org.apache.flink.state.api;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.state.api.utils.AggregateSum;
import org.apache.flink.state.api.utils.ReduceSum;
import org.apache.flink.state.api.utils.SavepointTestBase;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.List;

/**
 * IT Case for reading window operator state.
 */
public abstract class SavepointWindowReaderITCase<B extends StateBackend> extends SavepointTestBase {
	private static final String uid = "stateful-operator";

	private static final Integer[] numbers = { 1, 2, 3 };

	protected abstract B getStateBackend();

	@Test
	public void testReduceWindowStateReader() throws Exception {
		String savepointPath = takeSavepoint(numbers, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setStateBackend(getStateBackend());
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.assignTimestampsAndWatermarks(new ZeroTimestampAssigner<>())
				.keyBy(id -> id)
				.timeWindow(Time.milliseconds(10))
				.reduce(new ReduceSum())
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, getStateBackend());

		List<Integer> results = savepoint
			.timeWindow()
			.reduce(uid, new ReduceSum(), Types.INT, Types.INT)
			.collect();

		Assert.assertThat("Unexpected results from keyed state", results, Matchers.containsInAnyOrder(numbers));
	}

	@Test
	public void testReduceEvictorWindowStateReader() throws Exception {
		String savepointPath = takeSavepoint(numbers, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setStateBackend(getStateBackend());
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.assignTimestampsAndWatermarks(new ZeroTimestampAssigner<>())
				.keyBy(id -> id)
				.timeWindow(Time.milliseconds(10))
				.evictor(new NoOpEvictor<>())
				.reduce(new ReduceSum())
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, getStateBackend());

		List<Integer> results = savepoint
			.timeWindow()
			.evictor()
			.reduce(uid, new ReduceSum(), Types.INT, Types.INT)
			.collect();

		Assert.assertThat("Unexpected results from keyed state", results, Matchers.containsInAnyOrder(numbers));
	}

	@Test
	public void testAggregateWindowStateReader() throws Exception {
		String savepointPath = takeSavepoint(numbers, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setStateBackend(getStateBackend());
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.assignTimestampsAndWatermarks(new ZeroTimestampAssigner<>())
				.keyBy(id -> id)
				.timeWindow(Time.milliseconds(10))
				.aggregate(new AggregateSum())
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, getStateBackend());

		List<Integer> results = savepoint
			.timeWindow()
			.aggregate(uid, new AggregateSum(), Types.INT, Types.INT, Types.INT)
			.collect();

		Assert.assertThat("Unexpected results from keyed state", results, Matchers.containsInAnyOrder(numbers));
	}

	@Test
	public void testAggregateEvictorWindowStateReader() throws Exception {
		String savepointPath = takeSavepoint(numbers, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setStateBackend(getStateBackend());
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.assignTimestampsAndWatermarks(new ZeroTimestampAssigner<>())
				.keyBy(id -> id)
				.timeWindow(Time.milliseconds(10))
				.evictor(new NoOpEvictor<>())
				.aggregate(new AggregateSum())
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, getStateBackend());

		List<Integer> results = savepoint
			.timeWindow()
			.evictor()
			.aggregate(uid, new AggregateSum(), Types.INT, Types.INT, Types.INT)
			.collect();

		Assert.assertThat("Unexpected results from keyed state", results, Matchers.containsInAnyOrder(numbers));
	}

	@Test
	public void testProcessWindowStateReader() throws Exception {
		String savepointPath = takeSavepoint(numbers, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setStateBackend(getStateBackend());
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.assignTimestampsAndWatermarks(new ZeroTimestampAssigner<>())
				.keyBy(id -> id)
				.timeWindow(Time.milliseconds(10))
				.process(new NoOpProcessWindowFunction())
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, getStateBackend());

		List<Integer> results = savepoint
			.timeWindow()
			.process(uid, new BasicReaderFunction(), Types.INT, Types.INT, Types.INT)
			.collect();

		Assert.assertThat("Unexpected results from keyed state", results, Matchers.containsInAnyOrder(numbers));
	}

	@Test
	public void testProcessEvictorWindowStateReader() throws Exception {
		String savepointPath = takeSavepoint(numbers, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setStateBackend(getStateBackend());
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.assignTimestampsAndWatermarks(new ZeroTimestampAssigner<>())
				.keyBy(id -> id)
				.timeWindow(Time.milliseconds(10))
				.evictor(new NoOpEvictor<>())
				.process(new NoOpProcessWindowFunction())
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, getStateBackend());

		List<Integer> results = savepoint
			.timeWindow()
			.evictor()
			.process(uid, new BasicReaderFunction(), Types.INT, Types.INT, Types.INT)
			.collect();

		Assert.assertThat("Unexpected results from keyed state", results, Matchers.containsInAnyOrder(numbers));
	}

	@Test
	public void testApplyWindowStateReader() throws Exception {
		String savepointPath = takeSavepoint(numbers, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setStateBackend(getStateBackend());
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.assignTimestampsAndWatermarks(new ZeroTimestampAssigner<>())
				.keyBy(id -> id)
				.timeWindow(Time.milliseconds(10))
				.apply(new NoOpWindowFunction())
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, getStateBackend());

		List<Integer> results = savepoint
			.timeWindow()
			.process(uid, new BasicReaderFunction(), Types.INT, Types.INT, Types.INT)
			.collect();

		Assert.assertThat("Unexpected results from keyed state", results, Matchers.containsInAnyOrder(numbers));
	}

	@Test
	public void testApplyEvictorWindowStateReader() throws Exception {
		String savepointPath = takeSavepoint(numbers, source -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setStateBackend(getStateBackend());
			env.setParallelism(4);

			env
				.addSource(source)
				.rebalance()
				.assignTimestampsAndWatermarks(new ZeroTimestampAssigner<>())
				.keyBy(id -> id)
				.timeWindow(Time.milliseconds(10))
				.evictor(new NoOpEvictor<>())
				.apply(new NoOpWindowFunction())
				.uid(uid)
				.addSink(new DiscardingSink<>());

			return env;
		});

		ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
		ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, getStateBackend());

		List<Integer> results = savepoint
			.timeWindow()
			.evictor()
			.process(uid, new BasicReaderFunction(), Types.INT, Types.INT, Types.INT)
			.collect();

		Assert.assertThat("Unexpected results from keyed state", results, Matchers.containsInAnyOrder(numbers));
	}

	private static class ZeroTimestampAssigner<T> implements AssignerWithPunctuatedWatermarks<T> {
		@Nullable
		@Override
		public Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp) {
			return null;
		}

		@Override
		public long extractTimestamp(T element, long previousElementTimestamp) {
			return 0;
		}
	}

	private static class NoOpProcessWindowFunction extends ProcessWindowFunction<Integer, Integer, Integer, TimeWindow> {

		@Override
		public void process(Integer integer, Context context, Iterable<Integer> elements, Collector<Integer> out) { }
	}

	private static class NoOpWindowFunction implements WindowFunction<Integer, Integer, Integer, TimeWindow> {

		@Override
		public void apply(Integer integer, TimeWindow window, Iterable<Integer> input, Collector<Integer> out) { }
	}

	private static class BasicReaderFunction extends WindowReaderFunction<Integer, Integer, Integer, TimeWindow> {

		@Override
		public void readWindow(Integer key, Context<TimeWindow> context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
			Assert.assertEquals("Unexpected window", new TimeWindow(0, 10), context.window());
			Assert.assertThat("Unexpected registered timers", context.registeredEventTimeTimers(), Matchers.contains(9L));

			out.collect(elements.iterator().next());
		}
	}

	private static class NoOpEvictor<W extends Window> implements Evictor<Integer, W> {

		@Override
		public void evictBefore(Iterable<TimestampedValue<Integer>> elements, int size, W window, EvictorContext evictorContext) {
		}

		@Override
		public void evictAfter(Iterable<TimestampedValue<Integer>> elements, int size, W window, EvictorContext evictorContext) {
		}
	}
}
