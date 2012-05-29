/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;
import schema.TPCH_Schema;
import components.DataSourceComponent;
import components.EquiJoinComponent;
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
import operators.ProjectOperator;
import operators.SelectOperator;
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

        SelectOperator selectionRegion = new SelectOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, REGION_NAME)
                ));

        ProjectOperator projectionRegion = new ProjectOperator(new int[]{0});

        DataSourceComponent relationRegion = new DataSourceComponent(
                "REGION",
                dataPath + "region" + extension,
                TPCH_Schema.region,
                _queryPlan).setHashIndexes(hashRegion)
                           .addOperator(selectionRegion)
                           .addOperator(projectionRegion);

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(2);

        ProjectOperator projectionNation = new ProjectOperator(new int[]{0, 1, 2});

        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension,
                TPCH_Schema.nation,
                _queryPlan).setHashIndexes(hashNation)
                           .addOperator(projectionNation);


        //-------------------------------------------------------------------------------------
        List<Integer> hashRN = Arrays.asList(0);

        ProjectOperator projectionRN = new ProjectOperator(new int[]{1, 2});

        EquiJoinComponent R_Njoin = new EquiJoinComponent(
                relationRegion,
                relationNation,
                _queryPlan).setHashIndexes(hashRN)
                           .addOperator(projectionRN);

        //-------------------------------------------------------------------------------------
        List<Integer> hashSupplier = Arrays.asList(1);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                TPCH_Schema.supplier,
                _queryPlan).setHashIndexes(hashSupplier)
                           .addOperator(projectionSupplier);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNS = Arrays.asList(2);

        ProjectOperator projectionRNS = new ProjectOperator(new int[]{0, 1, 2});

        EquiJoinComponent R_N_Sjoin = new EquiJoinComponent(
                R_Njoin,
                relationSupplier,
                _queryPlan).setHashIndexes(hashRNS)
                           .addOperator(projectionRNS);

        //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(1);

        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 2, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                TPCH_Schema.lineitem,
                _queryPlan).setHashIndexes(hashLineitem)
                           .addOperator(projectionLineitem);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNSL = Arrays.asList(0, 2);

        ProjectOperator projectionRNSL = new ProjectOperator(new int[]{0, 1, 3, 4, 5});

        EquiJoinComponent R_N_S_Ljoin = new EquiJoinComponent(
                R_N_Sjoin,
                relationLineitem,
                _queryPlan).setHashIndexes(hashRNSL)
                           .addOperator(projectionRNSL);

        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(0);

        ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0, 3});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension,
                TPCH_Schema.customer,
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
                TPCH_Schema.orders,
                _queryPlan).setHashIndexes(hashOrders)
                           .addOperator(selectionOrders)
                           .addOperator(projectionOrders);

        //-------------------------------------------------------------------------------------
        List<Integer> hashCO = Arrays.asList(0, 1);

        ProjectOperator projectionCO = new ProjectOperator(new int[]{1, 2});

        EquiJoinComponent C_Ojoin = new EquiJoinComponent(
                relationCustomer,
                relationOrders,
                _queryPlan).setHashIndexes(hashCO)
                           .addOperator(projectionCO);

        //-------------------------------------------------------------------------------------
        List<Integer> hashRNSLCO = Arrays.asList(0);

        ProjectOperator projectionRNSLCO = new ProjectOperator(new int[]{1, 3, 4});

        EquiJoinComponent R_N_S_L_C_Ojoin = new EquiJoinComponent(
                R_N_S_Ljoin,
                C_Ojoin,
                _queryPlan).setHashIndexes(hashRNSLCO)
                           .addOperator(projectionRNSLCO);

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
                _queryPlan).addOperator(aggOp);

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