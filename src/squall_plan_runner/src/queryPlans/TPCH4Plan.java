/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;
import components.DataSourceComponent;
import components.EquiJoinComponent;
import components.OperatorComponent;
import conversion.DateConversion;
import conversion.IntegerConversion;
import conversion.TypeConversion;
import expressions.ColumnReference;
import expressions.DateSum;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import operators.AggregateCountOperator;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.DistinctOperator;
import operators.ProjectOperator;
import operators.SelectOperator;
import org.apache.log4j.Logger;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;

public class TPCH4Plan {
    private static Logger LOG = Logger.getLogger(TPCH4Plan.class);

    private static final TypeConversion<Date> _dc = new DateConversion();
    private static final IntegerConversion _ic = new IntegerConversion();
    private QueryPlan _queryPlan = new QueryPlan();

    //query variables
    private static Date _date1, _date2;

    private static void computeDates(){
        // date2= date1 + 3 months
        String date1Str = "1993-07-01";
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

    public TPCH4Plan(String dataPath, String extension, Map conf){
        computeDates();

        //-------------------------------------------------------------------------------------
        List<Integer> hashOrders = Arrays.asList(0);

        SelectOperator selectionOrders = new SelectOperator(
                new BetweenPredicate(
                    new ColumnReference(_dc, 4),
                    true, new ValueSpecification(_dc, _date1),
                    false, new ValueSpecification(_dc, _date2)
                ));

        ProjectOperator projectionOrders = new ProjectOperator(new int[]{0, 5});

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                _queryPlan).setHashIndexes(hashOrders)
                           .addOperator(selectionOrders)
                           .addOperator(projectionOrders);

        //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(0);

        SelectOperator selectionLineitem = new SelectOperator(
                new ComparisonPredicate(
                    ComparisonPredicate.LESS_OP,
                    new ColumnReference(_dc, 11),
                    new ColumnReference(_dc, 12)
                ));

        DistinctOperator distinctLineitem = new DistinctOperator(conf, new int[]{0});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                _queryPlan).setHashIndexes(hashLineitem)
                           .addOperator(selectionLineitem)
                           .addOperator(distinctLineitem);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent O_Ljoin = new EquiJoinComponent(
                relationOrders,
                relationLineitem,
                _queryPlan).setHashIndexes(Arrays.asList(1));

        //-------------------------------------------------------------------------------------
        // set up aggregation function on a separate StormComponent(Bolt)
        AggregateOperator aggOp = new AggregateCountOperator(conf).setGroupByColumns(Arrays.asList(1));
        OperatorComponent finalComponent = new OperatorComponent(
                O_Ljoin,
                "FINAL_RESULT",
                _queryPlan).addOperator(aggOp);

        //-------------------------------------------------------------------------------------

        AggregateOperator overallAgg =
                    new AggregateSumOperator(_ic, new ColumnReference(_ic, 1), conf)
                        .setGroupByColumns(Arrays.asList(0));

        _queryPlan.setOverallAggregation(overallAgg);
    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}
