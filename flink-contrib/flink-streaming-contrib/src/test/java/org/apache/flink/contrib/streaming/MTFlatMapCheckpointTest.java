package org.apache.flink.contrib.streaming;

import java.util.Random;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Test to check MultiThreadedFlatMapFunction behaviour with checkpoint
 */
public class MTFlatMapCheckpointTest extends MTFlatMapTestBase {

	private final static Logger LOG = LoggerFactory.getLogger(MTFlatMapCheckpointTest.class);

	@Test
	public void testOutputCountWhenStreamFailed() throws Exception {
		final StreamExecutionEnvironment env = getEnvironment();

		final long CHECKPOINT_INTERVAL = 1000;
		env.enableCheckpointing(CHECKPOINT_INTERVAL);

		final int N = 300000 * PARALLELISM;
		DataStream<Long> stream = env.addSource(new LongSource(N));

		FlatMapFunction<Long, Long> func = new FlatMapFunction<Long, Long>() {
			@Override
			public void flatMap(Long value, Collector<Long> out) throws Exception {
				out.collect(value);
			}
		};

		final int POOL_SIZE = 2;
		MultiThreadedFlatMapFunction<Long, Long> mtFunc =
				new MultiThreadedFlatMapFunction<>(func, stream.getType(), POOL_SIZE);

		DataStream<Long> out = stream.flatMap(mtFunc).map(new FailureGenerator(200000, 300000));
		out.addSink(new OutputCounterSink());

		env.execute();

		long itemCount = 0;
		for (int i = 0 ; i < PARALLELISM; i++) {
			itemCount += OutputCounterSink.counters[i];
		}

		assertEquals("Wrong number of output", N, itemCount);
	}

	/**
	 * Checkpoint-aware datasource
	 */
	private static class LongSource extends RichSourceFunction<Long>
			implements ParallelSourceFunction<Long>, Checkpointed<Long> {

		private long index;
		private long step;
		private long limit;
		private boolean running;
		private boolean restoring = false;

		LongSource(long limit) {
			super();
			this.limit = limit;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			step = getRuntimeContext().getNumberOfParallelSubtasks();
			running = true;
			if (!restoring) {
				index = getRuntimeContext().getIndexOfThisSubtask();
			}
			restoring = false;
		}

		@Override
		public void close() throws Exception {
			super.close();
			running = false;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return index;
		}

		@Override
		public void restoreState(Long state) throws Exception {
			restoring = true;
			index = state;
		}

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			Object checkpointLock = ctx.getCheckpointLock();
			while (running && index < limit) {
				synchronized (checkpointLock) {
					ctx.collect(index);
					index += step;
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	/**
	 * Map function to generate failure during processing
	 */
	private static class FailureGenerator extends RichMapFunction<Long, Long>
			implements Checkpointed<Long> {

		private static volatile boolean failureTriggered = false;
		private long cnt = 0;
		private final int minFail;
		private final int maxFail;
		private transient long failPosition;

		FailureGenerator(int min, int max) {
			this.minFail = min;
			this.maxFail = max;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			Random r = new Random(getRuntimeContext().getIndexOfThisSubtask());
			this.failPosition = minFail + r.nextInt(maxFail - minFail) ;
		}

		@Override
		public Long map(Long value) throws Exception {
			cnt++;
			if (!failureTriggered && cnt >= failPosition) {
				failureTriggered = true;
				throw new Exception("Failure trigger");
			}
			return value;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return cnt;
		}

		@Override
		public void restoreState(Long state) throws Exception {
			cnt = state;
		}
	}

	/**
	 * Output sink to count the number of received item
	 */
	private static class OutputCounterSink extends RichSinkFunction<Long> implements Checkpointed<Long>{

		static long[] counters = new long[PARALLELISM];
		private boolean restoring = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			if (!restoring) {
				counters[getRuntimeContext().getIndexOfThisSubtask()] = 0;
			}
			restoring = false;
		}

		@Override
		public void invoke(Long value) throws Exception {
			counters[getRuntimeContext().getIndexOfThisSubtask()]++;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return counters[getRuntimeContext().getIndexOfThisSubtask()];
		}

		@Override
		public void restoreState(Long state) throws Exception {
			counters[getRuntimeContext().getIndexOfThisSubtask()] = state;
			restoring = true;
		}

	}
}
