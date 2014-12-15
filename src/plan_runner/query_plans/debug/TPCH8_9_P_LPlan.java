package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.operators.ProjectOperator;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryBuilder;

public class TPCH8_9_P_LPlan {
    private static Logger LOG = Logger.getLogger(TPCH8_9_P_LPlan.class);

    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    private static final TypeConversion<Date> _dateConv = new DateConversion();
    private static final StringConversion _sc = new StringConversion();
    
    private static final String COLOR = "%green%";

    private QueryBuilder _queryBuilder = new QueryBuilder();
    
    private static final IntegerConversion _ic = new IntegerConversion();	    

    public TPCH8_9_P_LPlan(String dataPath, String extension, Map conf){
        //-------------------------------------------------------------------------------------
        List<Integer> hashPart = Arrays.asList(0);

        ProjectOperator projectionPart = new ProjectOperator(new int[]{0});

        DataSourceComponent relationPart = new DataSourceComponent(
                "PART",
                dataPath + "part" + extension).setHashIndexes(hashPart)
//                           .addOperator(selectionPart)
                           .addOperator(projectionPart);
        _queryBuilder.add(relationPart);

        //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(1);

        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 1, 2, 4, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension).setHashIndexes(hashLineitem)
                           .addOperator(projectionLineitem);
        _queryBuilder.add(relationLineitem);
        
//        AggregateCountOperator agg= new AggregateCountOperator(conf);
        
        //-------------------------------------------------------------------------------------
        /*
        EquiJoinComponent P_Ljoin = new EquiJoinComponent(
				relationPart,
				relationLineitem,
				_queryPlan).setHashIndexes(Arrays.asList(0, 2))
//				.addOperator(agg)
				;
        P_Ljoin.setPrintOut(false);
        */
        
        ColumnReference colP = new ColumnReference(_ic, 0);
		ColumnReference colL = new ColumnReference(_ic, 1);
		ComparisonPredicate P_L_pred = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colP, colL);
        
		EquiJoinComponent P_Ljoin = new EquiJoinComponent(
				relationPart,
				relationLineitem).setHashIndexes(Arrays.asList(0, 2))
						   .setJoinPredicate(P_L_pred)
						   .addOperator(new ProjectOperator(new int[]{0, 1, 3, 4, 5, 6}))
				;
		_queryBuilder.add(P_Ljoin);
        P_Ljoin.setPrintOut(false);
        
        //-------------------------------------------------------------------------------------

    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
