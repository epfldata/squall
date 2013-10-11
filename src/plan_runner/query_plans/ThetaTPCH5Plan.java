package plan_runner.query_plans;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.components.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.components.ThetaJoinStaticComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.DateSum;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryPlan;
import plan_runner.query_plans.ThetaQueryPlansParameters;
import plan_runner.utilities.SystemParameters;

public class ThetaTPCH5Plan {
    private static Logger LOG = Logger.getLogger(ThetaTPCH5Plan.class);
    
    private static final IntegerConversion _ic = new IntegerConversion();

    private static final TypeConversion<Date> _dc = new DateConversion();
    private static final TypeConversion<String> _sc = new StringConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();

    private QueryPlan _queryPlan = new QueryPlan();

    //query variables
    private static Date _date1, _date2;
    private static final String REGION_NAME = "ASIA";

    private static void computeDates(){
        // date2 = date1 + 1 year
        String date1Str = "1994-01-01";
        int interval = 1;
        int unit = Calendar.YEAR;

        //setting _date1
        _date1 = _dc.fromString(date1Str);

        //setting _date2
        ValueExpression<Date> date1Ve, date2Ve;
        date1Ve = new ValueSpecification<Date>(_dc, _date1);
        date2Ve = new DateSum(date1Ve, unit, interval);
        _date2 = date2Ve.eval(null);
        // tuple is set to null since we are computing based on constants
    }

    public ThetaTPCH5Plan(String dataPath, String extension, Map conf){
        computeDates();
        int Theta_JoinType=ThetaQueryPlansParameters.getThetaJoinType(conf);
		boolean isBDB = false;
		if(SystemParameters.isExisting(conf, "DIP_IS_BDB")){
			isBDB = SystemParameters.getBoolean(conf, "DIP_IS_BDB");
		}
		
        //-------------------------------------------------------------------------------------
        List<Integer> hashRegion = Arrays.asList(0);

        SelectOperator selectionRegion = new SelectOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, REGION_NAME)
                ));

        ProjectOperator projectionRegion = new ProjectOperator(new int[]{0});

        DataSourceComponent relationRegion = new DataSourceComponent(
                "REGION",
                dataPath + "region" + extension,
                _queryPlan).setHashIndexes(hashRegion)
                           .addOperator(selectionRegion)
                           .addOperator(projectionRegion);

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(2);

        ProjectOperator projectionNation = new ProjectOperator(new int[]{0, 1, 2});

        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension,
                _queryPlan).setHashIndexes(hashNation)
                           .addOperator(projectionNation);


        //-------------------------------------------------------------------------------------
        List<Integer> hashRN = Arrays.asList(0);

        ProjectOperator projectionRN = new ProjectOperator(new int[]{1, 2});
        
        ColumnReference colR = new ColumnReference(_ic, 0);
		ColumnReference colN = new ColumnReference(_ic, 2);
		ComparisonPredicate R_N_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colR, colN);

        Component R_Njoin=null;
        
        if(Theta_JoinType==0){
        	R_Njoin = new ThetaJoinStaticComponent(
                    relationRegion,
                    relationNation,
                    _queryPlan).setHashIndexes(hashRN)
                               .addOperator(projectionRN)
                               .setJoinPredicate(R_N_comp);
        }
        else if(Theta_JoinType==1){
        	R_Njoin = new ThetaJoinDynamicComponentAdvisedEpochs(
                    relationRegion,
                    relationNation,
                    _queryPlan).setHashIndexes(hashRN)
                               .addOperator(projectionRN)
                               .setJoinPredicate(R_N_comp);
        }
        //-------------------------------------------------------------------------------------
        List<Integer> hashSupplier = Arrays.asList(1);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                _queryPlan).setHashIndexes(hashSupplier)
                           .addOperator(projectionSupplier);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNS = Arrays.asList(2);

        ProjectOperator projectionRNS = new ProjectOperator(new int[]{0, 1, 2});

        ColumnReference colR_N = new ColumnReference(_ic, 0);
		ColumnReference colS = new ColumnReference(_ic, 1);
		ComparisonPredicate R_N_S_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colR_N, colS);
        
        Component R_N_Sjoin=null;
        
        if(Theta_JoinType==0){
        	R_N_Sjoin = new ThetaJoinStaticComponent(
        			R_Njoin,
                    relationSupplier,
                    _queryPlan).setHashIndexes(hashRNS)
                               .addOperator(projectionRNS)
                               .setJoinPredicate(R_N_S_comp);
        }
        else if(Theta_JoinType==1){
        	R_N_Sjoin = new ThetaJoinDynamicComponentAdvisedEpochs(
        			R_Njoin,
                    relationSupplier,
                    _queryPlan).setHashIndexes(hashRNS)
                               .addOperator(projectionRNS)
                               .setJoinPredicate(R_N_S_comp);
        }

        //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(1);

        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 2, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                _queryPlan).setHashIndexes(hashLineitem)
                           .addOperator(projectionLineitem);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNSL = Arrays.asList(0, 2);
        
        ColumnReference colR_N_S = new ColumnReference(_ic, 2);
		ColumnReference colL = new ColumnReference(_ic, 1);
		ComparisonPredicate R_N_S_L_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colR_N_S, colL);

        ProjectOperator projectionRNSL = new ProjectOperator(new int[]{0, 1, 3, 5, 6});

        Component R_N_S_Ljoin=null;
        
//        AggregateCountOperator agg= new AggregateCountOperator(conf);
        
        if(Theta_JoinType==0){
        	R_N_S_Ljoin = new ThetaJoinStaticComponent(
        			R_N_Sjoin,
                    relationLineitem,
                    _queryPlan).setHashIndexes(hashRNSL)
                               .addOperator(projectionRNSL)
                               .setJoinPredicate(R_N_S_L_comp)
//                               .addOperator(agg)
                               ;
        }
        else if(Theta_JoinType==1){
        	R_N_S_Ljoin = new ThetaJoinDynamicComponentAdvisedEpochs(
        			R_N_Sjoin,
                    relationLineitem,
                    _queryPlan).setHashIndexes(hashRNSL)
                               .addOperator(projectionRNSL)
                               .setJoinPredicate(R_N_S_L_comp)
//                               .addOperator(agg)
                               ;
        }
        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(0);

        ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0, 3});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension,
                _queryPlan).setHashIndexes(hashCustomer)
                           .addOperator(projectionCustomer);

        //-------------------------------------------------------------------------------------
        List<Integer> hashOrders = Arrays.asList(1);

        SelectOperator selectionOrders = new SelectOperator(
                new BetweenPredicate(
                    new ColumnReference(_dc, 4),
                    true, new ValueSpecification(_dc, _date1),
                    false, new ValueSpecification(_dc, _date2)
                ));

        ProjectOperator projectionOrders = new ProjectOperator(new int[]{0, 1});

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                _queryPlan).setHashIndexes(hashOrders)
                           .addOperator(selectionOrders)
                           .addOperator(projectionOrders);

        //-------------------------------------------------------------------------------------
        List<Integer> hashCO = Arrays.asList(0, 1);

        ProjectOperator projectionCO = new ProjectOperator(new int[]{1, 2});
        
        ColumnReference colC = new ColumnReference(_ic, 0);
		ColumnReference colO = new ColumnReference(_ic, 1);
		ComparisonPredicate C_O_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colC, colO);
        
        Component C_Ojoin=null;
        
        if(Theta_JoinType==0){
        	C_Ojoin = new ThetaJoinStaticComponent(
        			relationCustomer,
                    relationOrders,
                    _queryPlan).setHashIndexes(hashCO)
                               .addOperator(projectionCO)
                               .setBDB(isBDB)
                               .setJoinPredicate(C_O_comp);
        }
        else if(Theta_JoinType==1){
        	C_Ojoin = new ThetaJoinDynamicComponentAdvisedEpochs(
        			relationCustomer,
                    relationOrders,
                    _queryPlan).setHashIndexes(hashCO)
                               .addOperator(projectionCO)
                               .setJoinPredicate(C_O_comp);
        }

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNSLCO = Arrays.asList(0);

        ProjectOperator projectionRNSLCO = new ProjectOperator(new int[]{1, 3, 4});
        
        ColumnReference colR_N_S_L1 = new ColumnReference(_ic, 0);
        ColumnReference colR_N_S_L2 = new ColumnReference(_ic, 2);
        ColumnReference colC_O1 = new ColumnReference(_ic, 0);
		ColumnReference colC_O2 = new ColumnReference(_ic, 1);
		
		//StringConcatenate colR_N_S_L= new StringConcatenate(colR_N_S_L1, colR_N_S_L2);
		//StringConcatenate colC_O= new StringConcatenate(colC_O1, colC_O2);
		//ComparisonPredicate R_N_S_L_C_O_comp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, colR_N_S_L, colC_O);
		
		ComparisonPredicate pred1 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colR_N_S_L1, colC_O1);
		ComparisonPredicate pred2 = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colR_N_S_L2, colC_O2);
		AndPredicate R_N_S_L_C_O_comp= new AndPredicate(pred1, pred2);

        Component R_N_S_L_C_Ojoin=null;
        
        if(Theta_JoinType==0){
        	R_N_S_L_C_Ojoin = new ThetaJoinStaticComponent(
        			R_N_S_Ljoin,
                    C_Ojoin,
                    _queryPlan).setHashIndexes(hashRNSLCO)
                               .addOperator(projectionRNSLCO)
                               .setJoinPredicate(R_N_S_L_C_O_comp);
        }
        else if(Theta_JoinType==1){
        	R_N_S_L_C_Ojoin = new ThetaJoinDynamicComponentAdvisedEpochs(
        			R_N_S_Ljoin,
                    C_Ojoin,
                    _queryPlan).setHashIndexes(hashRNSLCO)
                               .addOperator(projectionRNSLCO)
                               .setJoinPredicate(R_N_S_L_C_O_comp);
        }

        //-------------------------------------------------------------------------------------
        // set up aggregation function on a separate StormComponent(Bolt)

        ValueExpression<Double> substract = new Subtraction(
                new ValueSpecification(_doubleConv, 1.0),
                new ColumnReference(_doubleConv, 2));
            //extendedPrice*(1-discount)
        ValueExpression<Double> product = new Multiplication(
                new ColumnReference(_doubleConv, 1),
                substract);

        AggregateOperator aggOp = new AggregateSumOperator(product, conf).setGroupByColumns(Arrays.asList(0));
        OperatorComponent finalComponent = new OperatorComponent(
                R_N_S_L_C_Ojoin,
                "FINAL_RESULT",
                _queryPlan).addOperator(aggOp);


        //-------------------------------------------------------------------------------------
    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}