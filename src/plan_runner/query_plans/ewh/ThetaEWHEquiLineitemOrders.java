package plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.theta.ThetaJoinComponentFactory;
import plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.theta.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DateIntegerConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.ewh.components.CreateHistogramComponent;
import plan_runner.ewh.components.DummyComponent;
import plan_runner.ewh.storm_components.DummyBolt;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.PrintOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.OrPredicate;
import plan_runner.query_plans.QueryPlan;
import plan_runner.query_plans.theta.ThetaQueryPlansParameters;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormEmitter;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;

public class ThetaEWHEquiLineitemOrders {
	private QueryPlan _queryPlan = new QueryPlan();
	private static final IntegerConversion _ic = new IntegerConversion();
	private static final StringConversion _stringConv = new StringConversion();

	public ThetaEWHEquiLineitemOrders(String dataPath, String extension, Map conf) {
		// there is no output skew, as the output is equal to the size of LINEITEM relation
		//   so equi-depth histogram is at the same time equi-weight histogram
		
		// materialized and non-materialized are the same
		/*
		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		PrintOperator print1 = printSelected? new PrintOperator("bci_1.tbl", conf) : null;
		PrintOperator print2 = printSelected? new PrintOperator("bci_2.tbl", conf) : null;
		// read from materialized relations
		boolean isMaterialized = SystemParameters.isExisting(conf, "DIP_MATERIALIZED") && SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
		*/
        boolean isOkcanSampling = SystemParameters.isExisting(conf, "DIP_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_SAMPLING");
        boolean isEWHSampling = SystemParameters.isExisting(conf, "DIP_EWH_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");
		
		ProjectOperator projectionLineitem = new ProjectOperator(new int[] {0});
		ProjectOperator projectionOrders = new ProjectOperator(new int[] {0});
		final List<Integer> hashLineitem = Arrays.asList(0);
		final List<Integer> hashOrders = Arrays.asList(0);
		
		// you could also try to use L_SHIPINSTRUCT(4 different) or L_SHIPMODE (7 different values) (both are strings)
		/*
		 * 36-6
		SelectOperator selectionLineitem = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, new ColumnReference(_ic, 3),
				new ValueSpecification(_ic, 4)));
				*/
		/*
		 * 8-15
		SelectOperator selectionLineitem = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, new ColumnReference(_stringConv, 14),
				new ValueSpecification(_stringConv, "TRUCK")));
				*/
		// 15-15
		SelectOperator selectionLineitem = new SelectOperator(new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, new ColumnReference(_ic, 3),
				new ValueSpecification(_ic, 2)));
		DataSourceComponent relationLineitem = new DataSourceComponent("LINEITEM", dataPath
				+ "lineitem" + extension, _queryPlan).addOperator(selectionLineitem).addOperator(projectionLineitem).setHashIndexes(hashLineitem);

		/*
		 * 36-6
		ComparisonPredicate selectionOrdersA = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "4-NOT SPECIFIED"));
		ComparisonPredicate selectionOrdersB = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP,
				new ColumnReference(_stringConv, 5), new ValueSpecification(_stringConv, "5-LOW"));
		OrPredicate orOrders = new OrPredicate(selectionOrdersA, selectionOrdersB);
		SelectOperator selectionOrders = new SelectOperator(orOrders);
		.addOperator(selectionOrders)
		*/
		DataSourceComponent relationOrders = new DataSourceComponent("ORDERS", dataPath
				+ "orders" + extension, _queryPlan).addOperator(projectionOrders).setHashIndexes(hashOrders);
		

		NumericConversion keyType = (NumericConversion) _ic;
		ComparisonPredicate comparison = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP);
		int firstKeyProject = 0;
		int secondKeyProject = 0;
		
		if(isOkcanSampling){
			_queryPlan = MyUtilities.addOkcanSampler(relationLineitem, relationOrders, firstKeyProject, secondKeyProject,
					_queryPlan, keyType, comparison, conf);
		}else if(isEWHSampling){
			_queryPlan = MyUtilities.addEWHSampler(relationLineitem, relationOrders, firstKeyProject, secondKeyProject,
					_queryPlan, keyType, comparison, conf); 
		}else{
			final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
			final ColumnReference colL = new ColumnReference(keyType, firstKeyProject);
			final ColumnReference colO = new ColumnReference(keyType, secondKeyProject);
			final ComparisonPredicate L_O_comp = new ComparisonPredicate(
					ComparisonPredicate.EQUAL_OP, colL, colO);

			//AggregateCountOperator agg = new AggregateCountOperator(conf);
			Component lastJoiner = ThetaJoinComponentFactory
					.createThetaJoinOperator(Theta_JoinType, relationLineitem, relationOrders, _queryPlan)
					.setJoinPredicate(L_O_comp).setContentSensitiveThetaJoinWrapper(keyType);
			//.addOperator(agg)
			// lastJoiner.setPrintOut(false);
			
			DummyComponent dummy = new DummyComponent(lastJoiner, "DUMMY", _queryPlan);
		}

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}