package theta_schedulers;

import java.util.Map;

import junit.framework.TestCase;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestAggregatesCounter;
import backtype.storm.testing.TestGlobalCount;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TestWordCounter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SchedulerTest extends TestCase{

	public static final int TOP_SUPERVISORS=4;
	public static final int TOP_PORTS=3;
	public static final int TOP_WORKERS=TOP_SUPERVISORS * TOP_PORTS;
	public static final String SCHEDULER_CLASS="theta_schedulers.ReschufflerJoinerCollocateScheduler";
	public static final String TOP_NAME="special-topology";
	
	public static final String FIRST_COMP="1";
	public static final String SECOND_COMP=FIRST_COMP + ReschufflerJoinerCollocateScheduler.RESHUFFLER_EXT;
	public static final String THIRD_COMP="3";
	public static final String FORTH_COMP=THIRD_COMP + ReschufflerJoinerCollocateScheduler.RESHUFFLER_EXT;
	
	public void testBasicTopology() {
		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(TOP_SUPERVISORS);
		mkClusterParam.setPortsPerSupervisor(TOP_PORTS);
		Config daemonConf = new Config();		
		daemonConf.put(Config.STORM_SCHEDULER, SCHEDULER_CLASS);
		daemonConf.put(Config.TOPOLOGY_WORKERS, TOP_WORKERS);
		mkClusterParam.setDaemonConf(daemonConf);

		/**
		 * This is a combination of <code>Testing.withLocalCluster</code> and <code>Testing.withSimulatedTime</code>.
		 */
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(ILocalCluster cluster) {
				// build the test topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout(FIRST_COMP, new TestWordSpout(true), 3);
				builder.setBolt(SECOND_COMP, 
						new TestWordCounter(), 3).fieldsGrouping(FIRST_COMP, new Fields("word"));
				builder.setBolt(THIRD_COMP, new TestGlobalCount()).globalGrouping(FIRST_COMP);
				builder.setBolt(FORTH_COMP, new TestAggregatesCounter())
				.globalGrouping(SECOND_COMP);
				StormTopology topology = builder.createTopology();

				// complete the topology

				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockData(FIRST_COMP, new Values("nathan"),
						new Values("bob"), new Values("joey"), new Values(
								"nathan"));

				// prepare the config
				Config conf = new Config();
				//conf.setNumWorkers(TOP_WORKERS);

				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				completeTopologyParam.setTopologyName(TOP_NAME);

				Map result = Testing.completeTopology(cluster, topology,
						completeTopologyParam);

				// check whether the result is right
				assertTrue(Testing.multiseteq(new Values(new Values("nathan"),
						new Values("bob"), new Values("joey"), new Values(
								"nathan")), Testing.readTuples(result, FIRST_COMP)));
				assertTrue(Testing.multiseteq(new Values(new Values("nathan", 1),
						new Values("nathan", 2), new Values("bob", 1),
						new Values("joey", 1)), Testing.readTuples(result, SECOND_COMP)));
				assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
						new Values(3), new Values(4)), Testing.readTuples(
								result, THIRD_COMP)));
				assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
						new Values(3), new Values(4)), Testing.readTuples(
								result, FORTH_COMP)));
			}

		});
	}
	
}
