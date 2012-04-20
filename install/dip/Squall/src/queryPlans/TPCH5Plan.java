/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;
import schema.TPCH_Schema;
import components.DataSourceComponent;
import components.JoinComponent;
import components.OperatorComponent;
import conversion.DateConversion;
import conversion.DoubleConversion;
import conversion.NumericConversion;
import conversion.StringConversion;
import conversion.TypeConversion;
import expressions.ColumnReference;
import expressions.DateSum;
import expressions.Multiplication;
import expressions.Subtraction;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import org.apache.log4j.Logger;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;

public class TPCH5Plan {
    private static Logger LOG = Logger.getLogger(TPCH5Plan.class);

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

    public TPCH5Plan(String dataPath, String extension, Map conf){
        computeDates();

        //-------------------------------------------------------------------------------------
        List<Integer> hashRegion = Arrays.asList(0);

        SelectionOperator selectionRegion = new SelectionOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, REGION_NAME)
                ));

        ProjectionOperator projectionRegion = new ProjectionOperator(new int[]{0});

        DataSourceComponent relationRegion = new DataSourceComponent(
                "REGION",
                dataPath + "region" + extension,
                TPCH_Schema.region,
                _queryPlan).setHashIndexes(hashRegion)
                           .setSelection(selectionRegion)
                           .setProjection(projectionRegion);

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(2);

        ProjectionOperator projectionNation = new ProjectionOperator(new int[]{0, 1, 2});

        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension,
                TPCH_Schema.nation,
                _queryPlan).setHashIndexes(hashNation)
                           .setProjection(projectionNation);


        //-------------------------------------------------------------------------------------
        List<Integer> hashRN = Arrays.asList(0);

        ProjectionOperator projectionRN = new ProjectionOperator(new int[]{1, 2});

        JoinComponent R_Njoin = new JoinComponent(
                relationRegion,
                relationNation,
                _queryPlan).setHashIndexes(hashRN)
                           .setProjection(projectionRN);

        //-------------------------------------------------------------------------------------
        List<Integer> hashSupplier = Arrays.asList(1);

        ProjectionOperator projectionSupplier = new ProjectionOperator(new int[]{0, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                TPCH_Schema.supplier,
                _queryPlan).setHashIndexes(hashSupplier)
                           .setProjection(projectionSupplier);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNS = Arrays.asList(2);

        ProjectionOperator projectionRNS = new ProjectionOperator(new int[]{0, 1, 2});

        JoinComponent R_N_Sjoin = new JoinComponent(
                R_Njoin,
                relationSupplier,
                _queryPlan).setHashIndexes(hashRNS)
                           .setProjection(projectionRNS);

        //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(1);

        ProjectionOperator projectionLineitem = new ProjectionOperator(new int[]{0, 2, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                TPCH_Schema.lineitem,
                _queryPlan).setHashIndexes(hashLineitem)
                           .setProjection(projectionLineitem);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNSL = Arrays.asList(0, 2);

        ProjectionOperator projectionRNSL = new ProjectionOperator(new int[]{0, 1, 3, 4, 5});

        JoinComponent R_N_S_Ljoin = new JoinComponent(
                R_N_Sjoin,
                relationLineitem,
                _queryPlan).setHashIndexes(hashRNSL)
                           .setProjection(projectionRNSL);

        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(0);

        ProjectionOperator projectionCustomer = new ProjectionOperator(new int[]{0, 3});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension,
                TPCH_Schema.customer,
                _queryPlan).setHashIndexes(hashCustomer)
                           .setProjection(projectionCustomer);

        //-------------------------------------------------------------------------------------
        List<Integer> hashOrders = Arrays.asList(1);

        SelectionOperator selectionOrders = new SelectionOperator(
                new BetweenPredicate(
                    new ColumnReference(_dc, 4),
                    true, new ValueSpecification(_dc, _date1),
                    false, new ValueSpecification(_dc, _date2)
                ));

        ProjectionOperator projectionOrders = new ProjectionOperator(new int[]{0, 1});

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                TPCH_Schema.orders,
                _queryPlan).setHashIndexes(hashOrders)
                           .setSelection(selectionOrders)
                           .setProjection(projectionOrders);

        //-------------------------------------------------------------------------------------
        List<Integer> hashCO = Arrays.asList(0, 1);

        ProjectionOperator projectionCO = new ProjectionOperator(new int[]{1, 2});

        JoinComponent C_Ojoin = new JoinComponent(
                relationCustomer,
                relationOrders,
                _queryPlan).setHashIndexes(hashCO)
                           .setProjection(projectionCO);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNSLCO = Arrays.asList(0);

        ProjectionOperator projectionRNSLCO = new ProjectionOperator(new int[]{1, 3, 4});

        JoinComponent R_N_S_L_C_Ojoin = new JoinComponent(
                R_N_S_Ljoin,
                C_Ojoin,
                _queryPlan).setHashIndexes(hashRNSLCO)
                           .setProjection(projectionRNSLCO);

        //-------------------------------------------------------------------------------------
        // set up aggregation function on a separate StormComponent(Bolt)

        ValueExpression<Double> substract = new Subtraction(
                _doubleConv,
                new ValueSpecification(_doubleConv, 1.0),
                new ColumnReference(_doubleConv, 2));
            //extendedPrice*(1-discount)
        ValueExpression<Double> product = new Multiplication(
                _doubleConv,
                new ColumnReference(_doubleConv, 1),
                substract);

        AggregateOperator aggOp = new AggregateSumOperator(_doubleConv, product, conf).setGroupByColumns(Arrays.asList(0));
        OperatorComponent finalComponent = new OperatorComponent(
                R_N_S_L_C_Ojoin,
                "FINAL_RESULT",
                _queryPlan).setAggregation(aggOp);

        //-------------------------------------------------------------------------------------
        AggregateOperator overallAgg =
                    new AggregateSumOperator(_doubleConv, new ColumnReference(_doubleConv, 1), conf)
                        .setGroupByColumns(Arrays.asList(0));

        _queryPlan.setOverallAggregation(overallAgg);
    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}