package org.apache.flink.contrib.streaming;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for MultiThreadedFlatMapFunction test
 */
public class MTFlatMapTestBase {
	protected static ForkableFlinkMiniCluster cluster;
	protected static final int PARALLELISM = 4;

	@BeforeClass
	public static void setUpClass() {
		Configuration conf = new Configuration();
		conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
		conf.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
		cluster = new ForkableFlinkMiniCluster(conf, false);
		cluster.start();
	}

	@AfterClass
	public static void tearDownClass() {
		cluster.stop();
		cluster = null;
	}

	protected StreamExecutionEnvironment getEnvironment() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
				"localhost", cluster.getLeaderRPCPort());

		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();
		return env;
	}
}
