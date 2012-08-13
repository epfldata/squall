package plan_runner.query_plans;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
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

public class TPCH10Plan {
    private static Logger LOG = Logger.getLogger(TPCH10Plan.class);

    private static final TypeConversion<Date> _dc = new DateConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
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

        ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0, 1, 2, 3, 4, 5, 7});

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
        EquiJoinComponent C_Ojoin = new EquiJoinComponent(
				relationCustomer,
				relationOrders,
				_queryPlan).setHashIndexes(Arrays.asList(3));
        //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(0);

        ProjectOperator projectionNation = new ProjectOperator(new int[]{0, 1});

        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension,
                _queryPlan).setHashIndexes(hashNation)
                           .addOperator(projectionNation);
        //-------------------------------------------------------------------------------------

        EquiJoinComponent C_O_Njoin = new EquiJoinComponent(
				C_Ojoin,
				relationNation,
				_queryPlan).addOperator(new ProjectOperator(new int[]{0, 1, 2, 4, 5, 6, 7, 8}))
                                           .setHashIndexes(Arrays.asList(6));
        //-------------------------------------------------------------------------------------

        List<Integer> hashLineitem = Arrays.asList(0);

        SelectOperator selectionLineitem = new SelectOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 8),
                    new ValueSpecification(_sc, "R")
                ));

        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                _queryPlan).setHashIndexes(hashLineitem)
                           .addOperator(selectionLineitem)
                           .addOperator(projectionLineitem);

        //-------------------------------------------------------------------------------------
        // set up aggregation function on the StormComponent(Bolt) where join is performed

	//1 - discount
	ValueExpression<Double> substract = new Subtraction(
			new ValueSpecification(_doubleConv, 1.0),
			new ColumnReference(_doubleConv, 8));
	//extendedPrice*(1-discount)
	ValueExpression<Double> product = new Multiplication(
			new ColumnReference(_doubleConv, 7),
			substract);
	AggregateOperator agg = new AggregateSumOperator(product, conf)
		.setGroupByColumns(Arrays.asList(0, 1, 4, 6, 2, 3, 5));

        EquiJoinComponent C_O_N_Ljoin = new EquiJoinComponent(
				C_O_Njoin,
				relationLineitem,
				_queryPlan).addOperator(new ProjectOperator(new int[]{0, 1, 2, 3, 4, 5, 7, 8, 9}))
                                           .addOperator(agg);
        //-------------------------------------------------------------------------------------

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}
