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

package org.apache.flink.contrib.streaming;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Test case for MultiThreadedFlatMapFunction
 */
public class MTFlatMapBasicTest extends MTFlatMapTestBase {

	@Test
	public void testThreadUsage() throws IOException {
		final StreamExecutionEnvironment env = getEnvironment();

		final int N = 10000;
		DataStream<Long> stream = env.generateSequence(1, N);

		FlatMapFunction<Long, Tuple2<Long, Long>> func = new FlatMapFunction<Long, Tuple2<Long, Long>>() {
			@Override
			public void flatMap(Long value, Collector<Tuple2<Long, Long>> out) throws Exception {
				long threadId = Thread.currentThread().getId();
				out.collect(new Tuple2<>(value, threadId));
			}
		};

		final int POOL_SIZE = 3;
		MultiThreadedFlatMapFunction<Long, Tuple2<Long, Long>> mtFunc =
				new MultiThreadedFlatMapFunction<>(func, stream.getType(), POOL_SIZE);

		DataStream<Tuple2<Long, Long>> threadIdStream = stream.flatMap(mtFunc);
		Iterator<Tuple2<Long, Long>> threadIdIterator = DataStreamUtils.collect(threadIdStream);

		int itemCount = 0;
		Map<Long, Integer> threadCount = new HashMap<>();
		while (threadIdIterator.hasNext()) {
			Tuple2<Long, Long> item = threadIdIterator.next();
			int count = 1;
			if (threadCount.containsKey(item.f1)) {
				count += threadCount.get(item.f1);
			}
			threadCount.put(item.f1, count);
			itemCount++;
		}

		assertEquals("Wrong number of output", N, itemCount);
		assertEquals("Wrong number or thread", POOL_SIZE * PARALLELISM, threadCount.size());
	}


	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testExceptionInsideUdf() throws Exception {
		final StreamExecutionEnvironment env = getEnvironment();

		final int N = 10;
		final String exceptionCause = "UDF Exception. Item divisible by 5";
		DataStream<Long> stream = env.generateSequence(1, N);

		FlatMapFunction<Long, Long> func = new FlatMapFunction<Long, Long>() {
			@Override
			public void flatMap(Long value, Collector<Long> out) throws Exception {
				if (value % 5 == 0) {
					throw new Exception(exceptionCause);
				}
				out.collect(value);
			}
		};

		final int POOL_SIZE = 3;
		MultiThreadedFlatMapFunction<Long, Long> mtFunc =
				new MultiThreadedFlatMapFunction<>(func, stream.getType(), POOL_SIZE);

		DataStream<Long> threadIdStream = stream.flatMap(mtFunc);

		threadIdStream.addSink(new DiscardingSink<Long>());

		exception.expect(new BaseMatcher<Throwable>() {
			@Override
			public boolean matches(Object item) {
				// Exception inside UDF is wrap inside multiple other exception
				Throwable exc = (Throwable) item;
				while(exc.getCause() != null) {
					exc = exc.getCause();
				}
				return exc.getMessage().equals(exceptionCause);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("Inner exception with msg " + exceptionCause);
			}
		});

		env.execute();
	}

	@Test
	public void testCorrectNumberOfOutput() throws Exception {
		final StreamExecutionEnvironment env = getEnvironment();

		final int N = 10;
		final int multiplier = 100;
		DataStream<Long> stream = env.generateSequence(1, N);

		FlatMapFunction<Long, Long> func = new FlatMapFunction<Long, Long>() {
			@Override
			public void flatMap(Long value, Collector<Long> out) throws Exception {
				long start = (value - 1) * multiplier;
				long end = value * multiplier;
				for (long i = start; i < end; i++) {
					out.collect(i);
				}
			}
		};

		final int POOL_SIZE = 3;
		MultiThreadedFlatMapFunction<Long, Long> mtFunc =
				new MultiThreadedFlatMapFunction<>(func, stream.getType(), POOL_SIZE);

		DataStream<Long> threadIdStream = stream.flatMap(mtFunc);
		Iterator<Long> iterator = DataStreamUtils.collect(threadIdStream);

		Set<Long> output = new HashSet<>(N * multiplier);
		while(iterator.hasNext()) {
			output.add(iterator.next());
		}

		assertEquals("Wrong number of output", N * multiplier, output.size());
	}
}
