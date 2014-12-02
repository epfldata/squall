package plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import plan_runner.components.Component;
import plan_runner.components.InterchangingDataSourceComponent;
import plan_runner.components.ThetaJoinComponentFactory;
import plan_runner.components.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryPlan;
import plan_runner.query_plans.ThetaQueryPlansParameters;
import plan_runner.utilities.SystemParameters;

public class ThetaOrdersLineFluctuationsPlanInterDataSource {

	private QueryPlan _queryPlan = new QueryPlan();
	private static final IntegerConversion _ic = new IntegerConversion();
	private static final StringConversion _sc = new StringConversion();

	public ThetaOrdersLineFluctuationsPlanInterDataSource(String dataPath, String extension,
			Map conf) {

		int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);

		/**
		 * Multiplicative Factor
		 */
		int multFactor = SystemParameters.getInt(conf, "DIP_MULT_FACTOR");

		AndPredicate and = new AndPredicate(new ComparisonPredicate(
				ComparisonPredicate.NONEQUAL_OP, new ColumnReference(_sc, 5),
				new ValueSpecification(_sc, "5-LOW")), new ComparisonPredicate(
				ComparisonPredicate.NONEQUAL_OP, new ColumnReference(_sc, 5),
				new ValueSpecification(_sc, "1-URGENT")));
		SelectOperator selectionOrders = new SelectOperator(and);
		final List<Integer> hashLineitem = Arrays.asList(0);

		//Orders
		ProjectOperator projectionOrders = new ProjectOperator(new int[] { 0 });
		ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0 });
		InterchangingDataSourceComponent relOrdersLineItem = new InterchangingDataSourceComponent(
				"ORDERS-LINEITEM", dataPath + "orders" + extension, dataPath + "lineitem"
						+ extension, _queryPlan, multFactor).addOperatorRel1(selectionOrders)
				.addOperatorRel1(projectionOrders).addOperatorRel2(projectionLineitem).setHashIndexes(hashLineitem);

		ColumnReference colO = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 0);
		ComparisonPredicate O_L_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colO,
				colL);

		Component join = ThetaJoinComponentFactory.createThetaJoinOperator(Theta_JoinType,
				relOrdersLineItem, null, _queryPlan).setJoinPredicate(O_L_comp).setContentSensitiveThetaJoinWrapper(_ic);

		join.setPrintOut(false);

		/*
		 * Component LINEITEMS_LINEITEMSjoin = null;
		if(Theta_JoinType==0){
			LINEITEMS_LINEITEMSjoin = new ThetaJoinStaticComponent(
				relationLineitem1,
				relationLineitem2,
				_queryPlan).setJoinPredicate(pred5)
				//           .addOperator(agg)
				;
		}
		else if(Theta_JoinType==1){
			LINEITEMS_LINEITEMSjoin = new ThetaJoinDynamicComponentNaiive(
					relationLineitem1,
					relationLineitem2,
					_queryPlan, ThetaQueryPlansParameters.getThetaDynamicRefreshRate(conf)).setJoinPredicate(pred5)
				//	           .addOperator(agg)
			;
		}
		else if(Theta_JoinType==2){
			LINEITEMS_LINEITEMSjoin = new ThetaJoinDynamicComponentAdvised(
					relationLineitem1,
					relationLineitem2,
					_queryPlan, ThetaQueryPlansParameters.getThetaDynamicRefreshRate(conf)).setJoinPredicate(pred5)
				//	           .addOperator(agg)
			;
		}
		else if(Theta_JoinType==3){
			LINEITEMS_LINEITEMSjoin = new ThetaJoinDynamicComponentAdvisedEpochs(
					relationLineitem1,
					relationLineitem2,
					_queryPlan).setJoinPredicate(pred5)
			      //     .addOperator(agg)
			;
		}
		LINEITEMS_LINEITEMSjoin.setPrintOut(false);
		 * 
		 */

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}

}
