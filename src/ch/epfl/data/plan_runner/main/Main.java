package ch.epfl.data.plan_runner.main;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.ewh.components.DummyComponent;
import ch.epfl.data.plan_runner.query_plans.HyracksPlan;
import ch.epfl.data.plan_runner.query_plans.HyracksPreAggPlan;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.query_plans.RSTPlan;
import ch.epfl.data.plan_runner.query_plans.TPCH10Plan;
import ch.epfl.data.plan_runner.query_plans.TPCH3Plan;
import ch.epfl.data.plan_runner.query_plans.TPCH4Plan;
import ch.epfl.data.plan_runner.query_plans.TPCH5Plan;
import ch.epfl.data.plan_runner.query_plans.TPCH7Plan;
import ch.epfl.data.plan_runner.query_plans.TPCH8Plan;
import ch.epfl.data.plan_runner.query_plans.TPCH9Plan;
import ch.epfl.data.plan_runner.query_plans.debug.HyracksL1Plan;
import ch.epfl.data.plan_runner.query_plans.debug.HyracksL3BatchPlan;
import ch.epfl.data.plan_runner.query_plans.debug.HyracksL3Plan;
import ch.epfl.data.plan_runner.query_plans.debug.TPCH3L1Plan;
import ch.epfl.data.plan_runner.query_plans.debug.TPCH3L23Plan;
import ch.epfl.data.plan_runner.query_plans.debug.TPCH3L2Plan;
import ch.epfl.data.plan_runner.query_plans.debug.TPCH5PlanAvg;
import ch.epfl.data.plan_runner.query_plans.debug.TPCH5_R_N_S_LPlan;
import ch.epfl.data.plan_runner.query_plans.debug.TPCH7_L_S_N1Plan;
import ch.epfl.data.plan_runner.query_plans.debug.TPCH8_9_P_LPlan;
import ch.epfl.data.plan_runner.query_plans.debug.ThetaLineitemSelfJoinInputDominated2_32;
import ch.epfl.data.plan_runner.query_plans.debug.ThetaLineitemSelfJoinInputDominated4_16;
import ch.epfl.data.plan_runner.query_plans.debug.ThetaLineitemSelfJoinInputDominated8_8;
import ch.epfl.data.plan_runner.query_plans.debug.ThetaOrdersLineFluctuationsPlanInterDataSource;
import ch.epfl.data.plan_runner.query_plans.debug.ThetaTPCH5_R_N_S_LPlan;
import ch.epfl.data.plan_runner.query_plans.debug.ThetaTPCH7_L_S_N1Plan;
import ch.epfl.data.plan_runner.query_plans.debug.ThetaTPCH8_9_P_LPlan;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHBandJPS;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHBandLineitemSelfOrderkeyJoin;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHBandOrdersCustkeyCustkeyJoin;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHBandOrdersOrderkeyCustkeyJoin;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHBandPeer;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHCustomerJoin;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHEquiLineitemOrders;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHEquiOrdersCustkeyCustkeyJoin;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHLineitemSelfOutputDominatedJoin;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHOrdersScaleJoin;
import ch.epfl.data.plan_runner.query_plans.ewh.ThetaEWHPartSuppJoin;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaHyracksPlan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaInputDominatedPlan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaLineitemPricesSelfJoin;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaLineitemSelfJoin;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaLineitemSelfJoinInputDominated;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaMultipleJoinPlan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaOrdersSelfJoin;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaOutputDominatedPlan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaTPCH10Plan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaTPCH3Plan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaTPCH4Plan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaTPCH5Plan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaTPCH7Plan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaTPCH8Plan;
import ch.epfl.data.plan_runner.query_plans.theta.ThetaTPCH9Plan;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.StormJoin;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.StormWrapper;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class Main {
	private static void addVariablesToMap(Map map, String confPath) {
		// setting topologyName: DIP_TOPOLOGY_NAME_PREFIX + CONFIG_FILE_NAME
		String confFilename = MyUtilities.getPartFromEnd(confPath, 0);
		String prefix = SystemParameters.getString(map,
				"DIP_TOPOLOGY_NAME_PREFIX");
		String topologyName = prefix + "_" + confFilename;
		SystemParameters.putInMap(map, "DIP_TOPOLOGY_NAME", topologyName);
	}

	public static QueryBuilder chooseQueryPlan(Map conf) {
		String queryName = SystemParameters.getString(conf, "DIP_QUERY_NAME");
		// if "/" is the last character, adding one more is not a problem
		String dataPath = SystemParameters.getString(conf, "DIP_DATA_PATH")
				+ "/";
		String extension = SystemParameters.getString(conf, "DIP_EXTENSION");
		boolean isMaterialized = SystemParameters.isExisting(conf,
				"DIP_MATERIALIZED")
				&& SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
		boolean isOkcanSampling = SystemParameters.isExisting(conf,
				"DIP_SAMPLING")
				&& SystemParameters.getBoolean(conf, "DIP_SAMPLING");
		boolean isEWHSampling = SystemParameters.isExisting(conf,
				"DIP_EWH_SAMPLING")
				&& SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");

		QueryBuilder queryPlan = null;
		/*
		 * if(isSampling && isMaterialized){ // still can be used when the query
		 * plan code is not adjusted, and the query is materialized queryPlan =
		 * new OkcanSampleMatrixPlan(dataPath, extension, conf).getQueryPlan();
		 * }else if (isEWHSampling && isMaterialized){ // still can be used when
		 * the query plan code is not adjusted, and the query is materialized
		 * queryPlan = new EWHSampleMatrixPlan(dataPath, extension,
		 * conf).getQueryPlan(); } else{
		 */
		// change between this and ...
		if (queryName.equalsIgnoreCase("rst")) {
			queryPlan = new RSTPlan(dataPath, extension, conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("hyracks")) {
			queryPlan = new HyracksPlan(conf).getQueryBuilder();
		} else if (queryName.equalsIgnoreCase("hyracks_pre_agg")) {
			queryPlan = new HyracksPreAggPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("hyracks_l1")) {
			queryPlan = new HyracksL1Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("hyracks_l3")) {
			queryPlan = new HyracksL3Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("hyracks_l3_batch")) {
			queryPlan = new HyracksL3BatchPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch3")) {
			queryPlan = new TPCH3Plan(dataPath, extension, conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tcph3_l1")) {
			queryPlan = new TPCH3L1Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch3_l2")) {
			queryPlan = new TPCH3L2Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch3_l23")) {
			queryPlan = new TPCH3L23Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch4")) {
			queryPlan = new TPCH4Plan(dataPath, extension, conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch5")) {
			queryPlan = new TPCH5Plan(dataPath, extension, conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch5avg")) {
			queryPlan = new TPCH5PlanAvg(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch7")) {
			queryPlan = new TPCH7Plan(dataPath, extension, conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch8")) {
			queryPlan = new TPCH8Plan(dataPath, extension, conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch9")) {
			queryPlan = new TPCH9Plan(dataPath, extension, conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch10")) {
			queryPlan = new TPCH10Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch5_R_N_S_L")) {
			queryPlan = new TPCH5_R_N_S_LPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch7_L_S_N1")) {
			queryPlan = new TPCH7_L_S_N1Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("tpch8_9_P_L")) {
			queryPlan = new TPCH8_9_P_LPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_input_dominated")) {
			queryPlan = new ThetaInputDominatedPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_output_dominated")) {
			queryPlan = new ThetaOutputDominatedPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_multiple_join")) {
			queryPlan = new ThetaMultipleJoinPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_hyracks")) {
			queryPlan = new ThetaHyracksPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch3")) {
			queryPlan = new ThetaTPCH3Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch4")) {
			queryPlan = new ThetaTPCH4Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch5")) {
			queryPlan = new ThetaTPCH5Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch7")) {
			queryPlan = new ThetaTPCH7Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch8")) {
			queryPlan = new ThetaTPCH8Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch9")) {
			queryPlan = new ThetaTPCH9Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch10")) {
			queryPlan = new ThetaTPCH10Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch7_L_S_N1")) {
			queryPlan = new ThetaTPCH7_L_S_N1Plan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch5_R_N_S_L")) {
			queryPlan = new ThetaTPCH5_R_N_S_LPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_tpch8_9_P_L")) {
			queryPlan = new ThetaTPCH8_9_P_LPlan(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_orders_self_join")) {
			queryPlan = new ThetaOrdersSelfJoin(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_lines_self_join")) {
			queryPlan = new ThetaLineitemSelfJoin(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName
				.equalsIgnoreCase("theta_lines_self_join_input_dominated")) {
			queryPlan = new ThetaLineitemSelfJoinInputDominated(dataPath,
					extension, conf).getQueryPlan();
		} else if (queryName
				.equalsIgnoreCase("theta_lines_self_join_input_dominated2_32")) {
			queryPlan = new ThetaLineitemSelfJoinInputDominated2_32(dataPath,
					extension, conf).getQueryPlan();
		} else if (queryName
				.equalsIgnoreCase("theta_lines_self_join_input_dominated4_16")) {
			queryPlan = new ThetaLineitemSelfJoinInputDominated4_16(dataPath,
					extension, conf).getQueryPlan();
		} else if (queryName
				.equalsIgnoreCase("theta_lines_self_join_input_dominated8_8")) {
			queryPlan = new ThetaLineitemSelfJoinInputDominated8_8(dataPath,
					extension, conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_lines_self_join_prices")) {
			queryPlan = new ThetaLineitemPricesSelfJoin(dataPath, extension,
					conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("orders_line_fluctuations_join")) {
			queryPlan = new ThetaOrdersLineFluctuationsPlanInterDataSource(
					dataPath, extension, conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_lineitem_orders_join")) {
			queryPlan = new ThetaEWHEquiLineitemOrders(dataPath, extension,
					conf).getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_lines_self_ewh_join")) {
			queryPlan = new ThetaEWHLineitemSelfOutputDominatedJoin(dataPath,
					extension, conf).getQueryPlan();
		} else if (queryName
				.equalsIgnoreCase("theta_lines_self_input_dominated_ewh_join")) {
			queryPlan = new ThetaEWHBandLineitemSelfOrderkeyJoin(dataPath,
					extension, conf).getQueryPlan();
		} else if (queryName
				.equalsIgnoreCase("theta_ewh_orders_self_custkey_join")) {
			queryPlan = new ThetaEWHEquiOrdersCustkeyCustkeyJoin(dataPath,
					extension, conf).getQueryPlan();
		} else if (queryName
				.equalsIgnoreCase("theta_ewh_band_orders_self_custkey_join")) {
			queryPlan = new ThetaEWHBandOrdersCustkeyCustkeyJoin(dataPath,
					extension, conf).getQueryPlan();
		} else if (queryName
				.equalsIgnoreCase("theta_ewh_band_orderkey_custkey_join")) {
			queryPlan = new ThetaEWHBandOrdersOrderkeyCustkeyJoin(dataPath,
					extension, conf).getQueryPlan();
		} else if (queryName
				.equalsIgnoreCase("theta_ewh_band_peer_self_upload_download")) {
			queryPlan = new ThetaEWHBandPeer(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_ewh_band_jps")) {
			queryPlan = new ThetaEWHBandJPS(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_ewh_cust_phone")) {
			queryPlan = new ThetaEWHCustomerJoin(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_ewh_partsupp_qty")) {
			queryPlan = new ThetaEWHPartSuppJoin(dataPath, extension, conf)
					.getQueryPlan();
		} else if (queryName.equalsIgnoreCase("theta_ewh_orders_scale")) {
			queryPlan = new ThetaEWHOrdersScaleJoin(dataPath, extension, conf)
					.getQueryPlan();
		}

		// ... this line

		if (queryPlan == null) {
			throw new RuntimeException("QueryPlan " + queryName
					+ " doesn't exist in Main.java");
		}
		return queryPlan;
	}

	private static TopologyBuilder createTopology(QueryBuilder qp, Config conf) {
		TopologyBuilder builder = new TopologyBuilder();
		TopologyKiller killer = new TopologyKiller(builder);

		// DST_ORDERING is the optimized version, so it's used by default
		int partitioningType = StormJoin.DST_ORDERING;

		List<Component> queryPlan = qp.getPlan();
		List<String> allCompNames = qp.getComponentNames();
		Collections.sort(allCompNames);
		int planSize = queryPlan.size();
		for (int i = 0; i < planSize; i++) {
			Component component = queryPlan.get(i);
			Component child = component.getChild();
			if (child == null) {
				// a last component (it might be multiple of them)
				component.makeBolts(builder, killer, allCompNames, conf,
						partitioningType, StormComponent.FINAL_COMPONENT);
			} else if (child instanceof DummyComponent) {
				component.makeBolts(builder, killer, allCompNames, conf,
						partitioningType, StormComponent.NEXT_TO_DUMMY);
			} else {
				component.makeBolts(builder, killer, allCompNames, conf,
						partitioningType, StormComponent.INTERMEDIATE);
			}
		}

		// printing infoID information and returning the result
		// printInfoID(killer, queryPlan); commented out because IDs are now
		// desriptive names
		return builder;
	}

	public static void main(String[] args) {
		new Main(args);
	}

	private static void printInfoID(TopologyKiller killer,
			List<Component> queryPlan) {

		StringBuilder infoID = new StringBuilder("\n");
		if (killer != null) {
			infoID.append(killer.getInfoID());
			infoID.append("\n");
		}
		infoID.append("\n");

		// after creating bolt, ID of a component is known
		int planSize = queryPlan.size();
		for (int i = 0; i < planSize; i++) {
			Component component = queryPlan.get(i);
			infoID.append(component.getInfoID());
			infoID.append("\n\n");
		}

		LOG.info(infoID.toString());
	}

	// this method is a skeleton for more complex ones
	// an optimizer should do this in a smarter way
	private static void putBatchSizes(QueryBuilder plan, Map map) {
		if (SystemParameters.isExisting(map, "BATCH_SIZE")) {

			// if the batch mode is specified, but nothing is put in map yet
			// (because other than MANUAL_BATCH optimizer is used)
			String firstBatch = plan.getComponentNames().get(0) + "_BS";
			if (!SystemParameters.isExisting(map, firstBatch)) {
				String batchSize = SystemParameters
						.getString(map, "BATCH_SIZE");
				for (String compName : plan.getComponentNames()) {
					String batchStr = compName + "_BS";
					SystemParameters.putInMap(map, batchStr, batchSize);
				}
			}

			// no matter where this is set, we print out batch sizes of
			// components
			for (String compName : plan.getComponentNames()) {
				String batchStr = compName + "_BS";
				String batchSize = SystemParameters.getString(map, batchStr);
				LOG.info("Batch size for " + compName + " is " + batchSize);
			}
		}
		if (!MyUtilities.checkSendMode(map)) {
			throw new RuntimeException(
					"BATCH_SEND_MODE value is not recognized.");
		}
	}

	private static Logger LOG = Logger.getLogger(Main.class);

	public Main(QueryBuilder queryPlan, Map map, String confPath) {
		Config conf = SystemParameters.mapToStormConfig(map);

		addVariablesToMap(conf, confPath);
		putBatchSizes(queryPlan, conf);
		TopologyBuilder builder = createTopology(queryPlan, conf);
		StormWrapper.submitTopology(conf, builder);
	}

	public Main(String[] args) {
		String confPath = args[0];
		Config conf = SystemParameters.fileToStormConfig(confPath);
		QueryBuilder queryPlan = chooseQueryPlan(conf);

		// conf.put(conf.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 262144);
		// conf.put(conf.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 262144);
		// conf.put(conf.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		// conf.put(conf.TOPOLOGY_TRANSFER_BUFFER_SIZE, 262144);

		addVariablesToMap(conf, confPath);
		putBatchSizes(queryPlan, conf);
		TopologyBuilder builder = createTopology(queryPlan, conf);
		StormWrapper.submitTopology(conf, builder);
	}
}
