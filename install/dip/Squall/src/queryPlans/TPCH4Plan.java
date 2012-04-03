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
import conversion.TypeConversion;
import expressions.ColumnReference;
import expressions.DateSum;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import operators.AggregateCountOperator;
import operators.AggregateOperator;
import operators.DistinctOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import org.apache.log4j.Logger;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;

public class TPCH4Plan {
    private static Logger LOG = Logger.getLogger(TPCH4Plan.class);

    private static final TypeConversion<Date> _dc = new DateConversion();
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
        ArrayList<Integer> hashOrders = new ArrayList<Integer>(Arrays.asList(0));

        SelectionOperator selectionOrders = new SelectionOperator(
                new BetweenPredicate(
                    new ColumnReference(_dc, 4),
                    true, new ValueSpecification(_dc, _date1),
                    false, new ValueSpecification(_dc, _date2)
                ));

        ProjectionOperator projectionOrders = new ProjectionOperator(new int[]{0, 5});

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                TPCH_Schema.orders,
                _queryPlan).setHashIndexes(hashOrders)
                           .setSelection(selectionOrders)
                           .setProjection(projectionOrders);

        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashLineitem = new ArrayList<Integer>(Arrays.asList(0));

        SelectionOperator selectionLineitem = new SelectionOperator(
                new ComparisonPredicate(
                    ComparisonPredicate.LESS_OP,
                    new ColumnReference(_dc, 11),
                    new ColumnReference(_dc, 12)
                ));

        DistinctOperator distinctLineitem = new DistinctOperator(new int[]{0});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                TPCH_Schema.lineitem,
                _queryPlan).setHashIndexes(hashLineitem)
                           .setSelection(selectionLineitem)
                           .setDistinct(distinctLineitem);

        //-------------------------------------------------------------------------------------
        JoinComponent O_Ljoin = new JoinComponent(
                relationOrders,
                relationLineitem,
                _queryPlan);

        //-------------------------------------------------------------------------------------
        // set up aggregation function on a separate StormComponent(Bolt)
        AggregateOperator aggOp = new AggregateCountOperator(conf).setGroupByColumns(new ArrayList<Integer>(Arrays.asList(1)));
        OperatorComponent finalComponent = new OperatorComponent(
                O_Ljoin,
                "FINAL_RESULT",
                _queryPlan).setAggregation(aggOp);

        //-------------------------------------------------------------------------------------
    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}