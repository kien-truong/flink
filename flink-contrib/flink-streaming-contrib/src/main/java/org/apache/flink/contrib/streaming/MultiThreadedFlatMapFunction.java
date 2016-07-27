/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A multi-thread flat map function that executes synchronous FlatMapFunction in a thread pool.
 * Supplying an asynchronous FlatMapFunction will result in undefined behaviour/missing output
 */
public class MultiThreadedFlatMapFunction<T, O> extends RichFlatMapFunction<T, O>
		implements ResultTypeQueryable {

	private FlatMapFunction<T, O> udf;
	private TypeInformation outputType;
	private int poolSize;
	private transient ExecutorService pool;
	private transient Queue<Future<Boolean>> futureResults;

	private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedFlatMapFunction.class);

	public MultiThreadedFlatMapFunction(FlatMapFunction<T, O> udf, TypeInformation<T> inputType, int poolSize) {
		this.udf = udf;
		this.pool = null;
		this.poolSize = poolSize;

		// Clean the udf function so it's serializable
		ClosureCleaner.clean(udf, true);

		this.outputType = TypeExtractor.getFlatMapReturnTypes(udf, inputType);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		if (pool == null) {
			LOG.debug("Initializing thread pool with {} threads", poolSize);
			futureResults = new ArrayDeque<>(poolSize);
			pool = Executors.newFixedThreadPool(poolSize);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (pool != null) {
			LOG.debug("Shutting down thread pool");
			pool.shutdown();
			while (!futureResults.isEmpty()) {
				processResult();
			}
			pool = null;
			futureResults = null;
		}
	}

	@Override
	public void flatMap(T value, Collector<O> out) throws Exception {
		Future<Boolean> ret = pool.submit(new ThreadWorker<>(value, out, udf));
		futureResults.add(ret);

		if (futureResults.size() == poolSize) {
			// Would be much more elegant to use CompletableFuture,
			// but Java 7 doesn't have that
			processResult();
		}
	}

	/**
	 * Process the result of the worker thread to check for exception
	 *
	 * @throws InterruptedException
	 */
	private void processResult() throws InterruptedException {
		Future<Boolean> firstResult = futureResults.poll();
		if (firstResult == null) {
			return;
		}
		try {
			firstResult.get();
		} catch (ExecutionException e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public TypeInformation getProducedType() {
		return outputType;
	}

}

/**
 * Worker thread to run the user-defined FlatMapFunction
 *
 * @param <T>
 * @param <O>
 */
class ThreadWorker<T, O> implements Callable<Boolean> {

	private final T input;
	private final Collector<O> collector;
	private final FlatMapFunction<T, O> udf;

	ThreadWorker(T input, Collector<O> collector, FlatMapFunction<T, O> udf) {
		this.input = input;
		this.collector = collector;
		this.udf = udf;
	}

	@Override
	public Boolean call() throws Exception {
		Collector<O> myCollector = new ThreadCollector<>(collector);
		udf.flatMap(input, myCollector);
		// Assuming that the udf.flatmap is synchronous
		// ThreadCollector will ignore items after it's closed
		// Close the thread collector to flush the buffer
		myCollector.close();
		return true;
	}
}

/**
 * Temporary collector to buffer the output of the user-defined FlatMapFunction
 * This helps to avoid excessive locking on the global collector
 *
 * @param <O>
 */
class ThreadCollector<O> implements Collector<O> {

	private static final int BUFFER_SIZE = 32;
	private final Collector<O> parent;
	private int bufSize;
	private Boolean closed;

	@SuppressWarnings("unchecked")
	private O[] buffer = (O[]) new Object[BUFFER_SIZE];

	ThreadCollector(Collector<O> collector) {
		parent = collector;
		bufSize = 0;
		closed = false;
	}

	@Override
	public void collect(O o) {
		if (closed) {
			return;
		}
		buffer[bufSize] = o;
		bufSize++;
		if (bufSize == BUFFER_SIZE) {
			flush();
		}
	}

	@Override
	public void close() {
		closed = true;
		if (bufSize > 0) {
			flush();
		}
	}

	/**
	 * Flush the buffer, called when buffer is full or the collector is close
	 */
	private void flush() {
		synchronized (parent) {
			for (int i = 0; i < bufSize; i++) {
				O item = buffer[i];
				parent.collect(item);
			}
		}
		bufSize = 0;
	}
}
