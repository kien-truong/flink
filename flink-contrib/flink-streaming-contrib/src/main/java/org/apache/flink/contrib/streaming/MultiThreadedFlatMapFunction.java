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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A multi-thread flat map function that executes synchronous FlatMapFunction in a thread pool.
 * Supplying an asynchronous FlatMapFunction will result in undefined behaviour/missing output
 */
public class MultiThreadedFlatMapFunction<T, O> extends RichFlatMapFunction<T, O>
		implements ResultTypeQueryable, CheckpointListener, Checkpointed<byte[]> {

	private final FlatMapFunction<T, O> udf;
	private final TypeInformation outputType;
	private final TypeInformation<T> inputType;
	private final int poolSize;
	private boolean restoring = false;
	private transient ExecutorService pool;
	private transient ExecutorCompletionService<Tuple2<List<O>, Long>> poolWatcher;
	private transient Collector<O> collector;
	private transient HashMap<Long, T> idsInFlight;
	private transient int freeThread;
	private transient long udfIdCnt;

	private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedFlatMapFunction.class);

	/**
	 * @param udf The user-defined function that's used to process the input
	 * @param inputType The type of the input stream
	 * @param poolSize The size of the thread pool to use
	 * @param cleanUdf Set this to false to disable the usage of ClosureCleaner to clean the user-defined function.
	 */
	public MultiThreadedFlatMapFunction(
			FlatMapFunction<T, O> udf, TypeInformation<T> inputType, int poolSize, boolean cleanUdf) {
		this.udf = udf;
		this.pool = null;
		this.collector = null;
		this.poolSize = poolSize;
		this.inputType = inputType;

		if (cleanUdf) {
			// Clean the udf function so it's serializable
			ClosureCleaner.clean(udf, true);
		} else {
			ClosureCleaner.ensureSerializable(udf);
		}


		this.outputType = TypeExtractor.getFlatMapReturnTypes(udf, inputType);
	}

	/**
	 * @param udf The user-defined function that's used to process the input.
	 * @param inputType The type of the input stream
	 * @param poolSize The size of the thread pool to use
	 */
	public MultiThreadedFlatMapFunction(FlatMapFunction<T, O> udf, TypeInformation<T> inputType, int poolSize) {
		this(udf, inputType, poolSize, true);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		if (pool == null) {
			LOG.debug("Initializing thread pool with {} threads", poolSize);
			collector = null;
			freeThread = poolSize;
			pool = Executors.newFixedThreadPool(poolSize);
			poolWatcher = new ExecutorCompletionService<>(pool);
			if (udf instanceof AbstractRichFunction) {
				((AbstractRichFunction) udf).setRuntimeContext(getRuntimeContext());
				((AbstractRichFunction) udf).open(parameters);
			}
			if (!restoring) {
				udfIdCnt = 0;
				idsInFlight = new HashMap<>(poolSize);
			}
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (pool != null) {
			LOG.debug("Shutting down thread pool");
			pool.shutdown();
			while (freeThread < poolSize) {
				processResult(true);
			}
			pool = null;
			poolWatcher = null;
			udfIdCnt = 0;
			collector = null;
			if (udf instanceof AbstractRichFunction) {
				((AbstractRichFunction) udf).close();
			}
		}
	}

	@Override
	public void flatMap(T value, Collector<O> out) throws Exception {
		if (collector == null) {
			LOG.debug("Collector is set");
			collector = out;
		} else if (collector != out) {
			// Cannot triggered this code path in testing, probably due to optimizer
			// Left this here just in case
			throw new IllegalAccessException(
				"Collector cannot be changed when using MultiThreadFlatMapFunction due to checkpoint support. "
			);
		}

		if (restoring) {
			LOG.debug("Start processing checkpointed value");
			for(Map.Entry<Long, T> entry: idsInFlight.entrySet()) {
				poolWatcher.submit(new ThreadWorker<>(entry.getValue(), udf, entry.getKey()));
				freeThread--;
			}
			restoring = false;
			LOG.debug("Finish restoring");
		}
		poolWatcher.submit(new ThreadWorker<>(value, udf, udfIdCnt));
		idsInFlight.put(udfIdCnt, value);
		udfIdCnt++;
		freeThread--;

		// Restoring from snapshot can freeThread go negative,
		// but since out pool is fixed size it will not overload the system too much
		while (freeThread <= 0) {
			processResult(true);
		}

		//Try to process more results if available, but do not block waiting for them
		while(processResult(false)) {
			// Do nothing
		}
	}

	/**
	 * Process the result of the worker thread to check for exception
	 * @return true if we have successfully process a result, false if there're no result to process
	 *
	 * @throws InterruptedException
	 */
	private boolean processResult(boolean blocking) throws InterruptedException {
		Future<Tuple2<List<O>, Long>> firstResult;
		if (blocking) {
			firstResult = poolWatcher.take();
		} else {
			firstResult = poolWatcher.poll();
		}
		if (firstResult == null) {
			return false;
		}
		freeThread++;
		try {
			Tuple2<List<O>, Long> ret = firstResult.get();
			List<O> elements = ret.f0;
			long udfId = ret.f1;
			for (O e : elements) {
				collector.collect(e);
			}
			idsInFlight.remove(udfId);
			return true;
		} catch (ExecutionException e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public TypeInformation getProducedType() {
		return outputType;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.debug("Checkpoint completed");
	}

	@Override
	public byte[] snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		LOG.debug("Start snapshotting");
		// Need to store
		// 1. udfIdCnt
		// 2. idsInFlight
		TypeSerializer<T> inputSerializer = inputType.createSerializer(getRuntimeContext().getExecutionConfig());
		int length = inputSerializer.getLength();
		// Unknown length, allocate 16 byte
		if (length == -1) {
			length = 16;
		}
		DataOutputSerializer outputEnc = new DataOutputSerializer(8 + 4 + (length + 8) * idsInFlight.size());
		outputEnc.writeLong(udfIdCnt);
		LOG.debug("ID counter at: {}", udfIdCnt);
		outputEnc.writeInt(idsInFlight.size());
		LOG.debug("Saving {} in-flight inputs", idsInFlight.size());
		for(Map.Entry<Long, T> entry: idsInFlight.entrySet()) {
			outputEnc.writeLong(entry.getKey());
			inputSerializer.serialize(entry.getValue(), outputEnc);
		}
		LOG.debug("Finish snapshotting");
		return outputEnc.getByteArray();
	}

	@Override
	public void restoreState(byte[] state) throws Exception {
		LOG.debug("Start restoring");
		restoring = true;
		idsInFlight = new HashMap<>(poolSize);
		DataInputDeserializer inputDec = new DataInputDeserializer(state, 0, state.length);
		TypeSerializer<T> inputSerializer = inputType.createSerializer(getRuntimeContext().getExecutionConfig());
		udfIdCnt = inputDec.readLong();
		LOG.debug("ID counter set to: {}", udfIdCnt);
		int mapSize = inputDec.readInt();
		LOG.debug("Reading {} saved input", mapSize);
		for (int i = 0; i < mapSize; i++) {
			long id = inputDec.readLong();
			T item = inputSerializer.deserialize(inputDec);
			idsInFlight.put(id, item);
		}
		// The restore is not complete yet, it will be continued when flatMap is called and collector is set
	}
}

/**
 * Worker thread to run the user-defined FlatMapFunction
 *
 * @param <T>
 * @param <O>
 */
class ThreadWorker<T, O> implements Callable<Tuple2<List<O>, Long>> {

	private final T input;
	private final FlatMapFunction<T, O> udf;
	private final long id;

	ThreadWorker(T input, FlatMapFunction<T, O> udf, long id) {
		this.input = input;
		this.udf = udf;
		this.id = id;
	}

	@Override
	public Tuple2<List<O>, Long> call() throws Exception {
		ThreadCollector<O> myCollector = new ThreadCollector<>();
		udf.flatMap(input, myCollector);
		// Assuming that the udf.flatmap is synchronous
		// ThreadCollector will ignore items after it's closed
		myCollector.close();

		return new Tuple2<>(myCollector.getBuffer(), id);
	}
}

/**
 * Temporary collector to buffer the output of the user-defined FlatMapFunction
 * This helps to avoid excessive locking on the global collector
 *
 * @param <O>
 */
class ThreadCollector<O> implements Collector<O> {

	private Boolean closed;
	private List<O> buffer = new ArrayList<>(32);

	ThreadCollector() {
		closed = false;
	}

	@Override
	public void collect(O o) {
		if (closed) {
			return;
		}
		buffer.add(o);
	}

	@Override
	public void close() {
		closed = true;
	}

	List<O> getBuffer() {
		return buffer;
	}
}
