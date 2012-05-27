/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;
import schema.TPCH_Schema;
import components.DataSourceComponent;
import components.JoinComponent;
import conversion.DateConversion;
import conversion.DoubleConversion;
import conversion.IntegerConversion;
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

public class TPCH10Plan {
    private static Logger LOG = Logger.getLogger(TPCH10Plan.class);

    private static final TypeConversion<Date> _dc = new DateConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    private static final IntegerConversion _ic = new IntegerConversion();
    private static final StringConversion _sc = new StringConversion();
    private QueryPlan _queryPlan = new QueryPlan();

    //query variables
    private static Date _date1, _date2;

    private static void computeDates(){
        // date2= date1 + 3 months
        String date1Str = "1993-10-01";
        int interval = 3;
        int unit = Calendar.MONTH;

        //setting _date1
        _date1 = _dc.fromString(date1Str);

        //setting _date2
        ValueExpression<Date> date1Ve, date2Ve;
        date1Ve = new ValueSpecification<Date>(_dc, _date1);
        date2Ve = new DateSum(date1Ve, unit, interval);
        _date2 = date2Ve.eval(null);
        // tuple is set to null since we are computing based on constants
    }

    public TPCH10Plan(String dataPath, String extension, Map conf){
        computeDates();

        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(0);

        ProjectionOperator projectionCustomer = new ProjectionOperator(new int[]{0, 1, 2, 3, 4, 5, 7});

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
        JoinComponent C_Ojoin = new JoinComponent(
				relationCustomer,
				relationOrders,
				_queryPlan).setHashIndexes(Arrays.asList(3));
        //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(0);

        ProjectionOperator projectionNation = new ProjectionOperator(new int[]{0, 1});

        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension,
                TPCH_Schema.nation,
                _queryPlan).setHashIndexes(hashNation)
                           .setProjection(projectionNation);
        //-------------------------------------------------------------------------------------

        JoinComponent C_O_Njoin = new JoinComponent(
				C_Ojoin,
				relationNation,
				_queryPlan).setProjection(new ProjectionOperator(new int[]{0, 1, 2, 4, 5, 6, 7, 8}))
                                           .setHashIndexes(Arrays.asList(6));
        //-------------------------------------------------------------------------------------

        List<Integer> hashLineitem = Arrays.asList(0);

        SelectionOperator selectionLineitem = new SelectionOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 8),
                    new ValueSpecification(_sc, "R")
                ));

        ProjectionOperator projectionLineitem = new ProjectionOperator(new int[]{0, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                TPCH_Schema.lineitem,
                _queryPlan).setHashIndexes(hashLineitem)
                           .setSelection(selectionLineitem)
                           .setProjection(projectionLineitem);

        //-------------------------------------------------------------------------------------
        // set up aggregation function on the StormComponent(Bolt) where join is performed

	//1 - discount
	ValueExpression<Double> substract = new Subtraction(
			_doubleConv,
			new ValueSpecification(_doubleConv, 1.0),
			new ColumnReference(_doubleConv, 8));
	//extendedPrice*(1-discount)
	ValueExpression<Double> product = new Multiplication(
			_doubleConv,
			new ColumnReference(_doubleConv, 7),
			substract);
	AggregateOperator agg = new AggregateSumOperator(_doubleConv, product, conf)
		.setGroupByColumns(Arrays.asList(0, 1, 4, 6, 2, 3, 5));

        JoinComponent C_O_N_Ljoin = new JoinComponent(
				C_O_Njoin,
				relationLineitem,
				_queryPlan).setProjection(new ProjectionOperator(new int[]{0, 1, 2, 3, 4, 5, 7, 8, 9}))
                                           .setAggregation(agg);
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
