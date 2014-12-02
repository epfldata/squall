package plan_runner.query_plans.ewh;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.ThetaJoinComponentFactory;
import plan_runner.components.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DateIntegerConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.ewh.components.DummyComponent;
import plan_runner.expressions.Addition;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Division;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateCountOperator;
import plan_runner.operators.PrintOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.OrPredicate;
import plan_runner.query_plans.QueryPlan;
import plan_runner.query_plans.ThetaQueryPlansParameters;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;

public class ThetaEWHBandPeer {
	private QueryPlan _queryPlan = new QueryPlan();
	private static final TypeConversion<String> _stringConv = new StringConversion();
	private static final IntegerConversion _ic = new IntegerConversion();
	private DateIntegerConversion _dic = new DateIntegerConversion();

	public ThetaEWHBandPeer(String dataPath, String extension, Map conf) {
		// creates materialized relations
		boolean printSelected = MyUtilities.isPrintFilteredLast(conf);
		String matName1 = "peer1_1";
		String matName2 = "peer1_2";
		PrintOperator print1 = printSelected? new PrintOperator(matName1 + extension, conf) : null;
		PrintOperator print2 = printSelected? new PrintOperator(matName2 + extension, conf) : null;
		// read from materialized relations
		boolean isMaterialized = SystemParameters.isExisting(conf, "DIP_MATERIALIZED") && SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
        boolean isOkcanSampling = SystemParameters.isExisting(conf, "DIP_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_SAMPLING");
        boolean isEWHSampling = SystemParameters.isExisting(conf, "DIP_EWH_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");
		
		Component relationPeer1, relationPeer2;
		// Full schema is <id,peer_id,torrent_snapshot_id,upload_speed,download_speed,payload_upload_speed,payload_download_speed,total_upload,total_download,fail_count,hashfail_count,progress,created
		// total_upload is field 7, total_download is field 8 
		
		
		ColumnReference col1 = new ColumnReference(_ic, 0);
		ColumnReference col2 = new ColumnReference(_ic, 2);
		ColumnReference col3 = new ColumnReference(_ic, 7);
		ColumnReference col4 = new ColumnReference(_ic, 8);
		

		ProjectOperator projectionPeer1 = new ProjectOperator(col1, col2, col3, col4);
		ProjectOperator projectionPeer2 = new ProjectOperator(col1, col2, col3, col4);
		//total_upload is field 2, total_download is field 3 in projectionPeer1 and projectionPeer2 
		final List<Integer> hashPeer1 = Arrays.asList(2);
		final List<Integer> hashPeer2 = Arrays.asList(3);
		
		if(!isMaterialized){
			// eliminate inactive users
			// total_upload is field 7, total_download is field 8 in the base relation
			ComparisonPredicate sel11 = new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
					new ColumnReference(_ic, 7), new ValueSpecification(_ic, 0));
			SelectOperator selectionPeer1 = new SelectOperator(sel11);
			
			ComparisonPredicate sel12 = new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
					new ColumnReference(_ic, 8), new ValueSpecification(_ic, 0));
			SelectOperator selectionPeer2 = new SelectOperator(sel12);
			
			// build relations
			relationPeer1 = new DataSourceComponent("PEER1", dataPath
					+ "peersnapshot-01" + extension, _queryPlan).addOperator(selectionPeer1).addOperator(print1).addOperator(
					projectionPeer1).setHashIndexes(hashPeer1);
			
			relationPeer2 = new DataSourceComponent("PEER2", dataPath
					+ "peersnapshot-01" + extension, _queryPlan).addOperator(selectionPeer2).addOperator(print2).addOperator(
					projectionPeer2).setHashIndexes(hashPeer2);
		}else{
			relationPeer1 = new DataSourceComponent("PEER1", dataPath
					+ matName1 + extension, _queryPlan).addOperator(projectionPeer1).setHashIndexes(hashPeer1);

			relationPeer2 = new DataSourceComponent("PEER1", dataPath
					+ matName2 + extension, _queryPlan).addOperator(projectionPeer2).setHashIndexes(hashPeer2);
		}
	
		
		NumericConversion keyType = (NumericConversion) _ic;
		int comparisonValue = 2;
		ComparisonPredicate comparison = new ComparisonPredicate(ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, comparisonValue, keyType);
		int firstKeyProject = 2;
		int secondKeyProject = 3;
			
		if(printSelected){
			relationPeer1.setPrintOut(false);
			relationPeer2.setPrintOut(false);
		}else if(isOkcanSampling){
			_queryPlan = MyUtilities.addOkcanSampler(relationPeer1, relationPeer2, firstKeyProject, secondKeyProject,
					_queryPlan, keyType, comparison, conf);
		}else if(isEWHSampling){
			_queryPlan = MyUtilities.addEWHSampler(relationPeer1, relationPeer2, firstKeyProject, secondKeyProject,
					_queryPlan, keyType, comparison, conf); 
		}else{
			final int Theta_JoinType = ThetaQueryPlansParameters.getThetaJoinType(conf);
			final ColumnReference colO1 = new ColumnReference(keyType, firstKeyProject);
			final ColumnReference colO2 = new ColumnReference(keyType, secondKeyProject);
			
			ComparisonPredicate pred5 = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP, colO1,
					colO2, comparisonValue, ComparisonPredicate.BPLUSTREE);

			//AggregateCountOperator agg = new AggregateCountOperator(conf);		
			Component lastJoiner = ThetaJoinComponentFactory.createThetaJoinOperator(
					Theta_JoinType, relationPeer1, relationPeer2, _queryPlan).setJoinPredicate(
							pred5).setContentSensitiveThetaJoinWrapper(keyType)
							;
			// .addOperator(agg)
			// lastJoiner.setPrintOut(false);
			
			DummyComponent dummy = new DummyComponent(lastJoiner, "DUMMY", _queryPlan);
		}

	}

	public QueryPlan getQueryPlan() {
		return _queryPlan;
	}
}