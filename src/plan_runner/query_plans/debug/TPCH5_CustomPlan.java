package plan_runner.query_plans.debug;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
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
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.query_plans.QueryBuilder;

public class TPCH5_CustomPlan {
    private static Logger LOG = Logger.getLogger(TPCH5_CustomPlan.class);

    private static final TypeConversion<Date> _dc = new DateConversion();
    private static final TypeConversion<String> _sc = new StringConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();

    private QueryBuilder _queryBuilder = new QueryBuilder();

    //query variables
    private static Date _date1, _date2;
    //private static final String REGION_NAME = "ASIA";
    private static final String REGION_NAME = "AMERICA";

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

    public TPCH5_CustomPlan(String dataPath, String extension, Map conf){
        computeDates();

        //-------------------------------------------------------------------------------------
        List<Integer> hashRegion = Arrays.asList(0);

        SelectOperator selectionRegion = new SelectOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, REGION_NAME)
                ));

        ProjectOperator projectionRegion = new ProjectOperator(new int[]{0});

        DataSourceComponent relationRegion = new DataSourceComponent(
                "REGION", dataPath + "region" + extension).setHashIndexes(hashRegion)
                           .addOperator(selectionRegion)
                           .addOperator(projectionRegion);
        _queryBuilder.add(relationRegion);

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(2);

        ProjectOperator projectionNation = new ProjectOperator(new int[]{0, 1, 2});

        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension).setHashIndexes(hashNation)
                           .addOperator(projectionNation);
        _queryBuilder.add(relationNation);


        //-------------------------------------------------------------------------------------
        List<Integer> hashRN = Arrays.asList(0);

        ProjectOperator projectionRN = new ProjectOperator(new int[]{1, 2});

        EquiJoinComponent R_Njoin = new EquiJoinComponent(
                relationRegion,
                relationNation).setHashIndexes(hashRN)
                           .addOperator(projectionRN);
        _queryBuilder.add(R_Njoin);

        //-------------------------------------------------------------------------------------
        List<Integer> hashSupplier = Arrays.asList(1);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension).setHashIndexes(hashSupplier)
                           .addOperator(projectionSupplier);
        _queryBuilder.add(relationSupplier);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNS = Arrays.asList(2);

        ProjectOperator projectionRNS = new ProjectOperator(new int[]{0, 1, 2});

        EquiJoinComponent R_N_Sjoin = new EquiJoinComponent(
                R_Njoin,
                relationSupplier).setHashIndexes(hashRNS)
                           .addOperator(projectionRNS);
        _queryBuilder.add(R_N_Sjoin);

        //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(1);

        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 2, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension).setHashIndexes(hashLineitem)
                           .addOperator(projectionLineitem);
        _queryBuilder.add(relationLineitem);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNSL = Arrays.asList(0, 2);

        ProjectOperator projectionRNSL = new ProjectOperator(new int[]{0, 1, 3, 4, 5});

        
//        AggregateCountOperator agg= new AggregateCountOperator(conf);
        
        EquiJoinComponent R_N_S_Ljoin = new EquiJoinComponent(
                R_N_Sjoin,
                relationLineitem).setHashIndexes(hashRNSL)
                           .addOperator(projectionRNSL)
//                           .addOperator(agg)
                           ;
        _queryBuilder.add(R_N_S_Ljoin);

        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(0);

        ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0, 3});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension).setHashIndexes(hashCustomer)
                           .addOperator(projectionCustomer);
        _queryBuilder.add(relationCustomer);

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
                dataPath + "orders" + extension).setHashIndexes(hashOrders)
                           .addOperator(selectionOrders)
                           .addOperator(projectionOrders);
        _queryBuilder.add(relationOrders);

        //-------------------------------------------------------------------------------------
        List<Integer> hashCO = Arrays.asList(0, 1);

        ProjectOperator projectionCO = new ProjectOperator(new int[]{1, 2});

        EquiJoinComponent C_Ojoin = new EquiJoinComponent(
                relationCustomer,
                relationOrders).setHashIndexes(hashCO)
                           .addOperator(projectionCO);
        _queryBuilder.add(C_Ojoin);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNSLCO = Arrays.asList(0);

        ProjectOperator projectionRNSLCO = new ProjectOperator(new int[]{1, 3, 4});

        EquiJoinComponent R_N_S_L_C_Ojoin = new EquiJoinComponent(
                R_N_S_Ljoin,
                C_Ojoin).setHashIndexes(hashRNSLCO)
                           .addOperator(projectionRNSLCO);
        _queryBuilder.add(R_N_S_L_C_Ojoin);

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
                "FINAL_RESULT").addOperator(aggOp);
        _queryBuilder.add(finalComponent);

        //-------------------------------------------------------------------------------------
    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}