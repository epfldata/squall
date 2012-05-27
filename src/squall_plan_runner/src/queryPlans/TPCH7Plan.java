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
import conversion.NumericConversion;
import conversion.StringConversion;
import conversion.TypeConversion;
import expressions.ColumnReference;
import expressions.IntegerYearFromDate;
import expressions.Multiplication;
import expressions.Subtraction;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import org.apache.log4j.Logger;
import predicates.AndPredicate;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;
import predicates.OrPredicate;

public class TPCH7Plan {
    private static Logger LOG = Logger.getLogger(TPCH7Plan.class);

    private QueryPlan _queryPlan = new QueryPlan();

    private static final String    _date1Str = "1995-01-01";
    private static final String    _date2Str = "1996-12-31";
    private static final String  _firstCountryName = "FRANCE";
    private static final String _secondCountryName = "GERMANY";

    private static final TypeConversion<Date> _dateConv = new DateConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    private static final TypeConversion<String> _sc = new StringConversion();
    private static final Date _date1=_dateConv.fromString(_date1Str);
    private static final Date _date2=_dateConv.fromString(_date2Str);

    public TPCH7Plan(String dataPath, String extension, Map conf){

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation1 = Arrays.asList(1);

        SelectionOperator selectionNation1 = new SelectionOperator(
                new OrPredicate(
                    new ComparisonPredicate(
                        new ColumnReference(_sc, 1),
                        new ValueSpecification(_sc, _firstCountryName)
                    ), new ComparisonPredicate(
                        new ColumnReference(_sc, 1),
                        new ValueSpecification(_sc, _secondCountryName)
                    )
                ));

        ProjectionOperator projectionNation1 = new ProjectionOperator(new int[]{1, 0});

        DataSourceComponent relationNation1 = new DataSourceComponent(
                "NATION1",
                dataPath + "nation" + extension,
                TPCH_Schema.nation,
                _queryPlan).setHashIndexes(hashNation1)
                           .setSelection(selectionNation1)
                           .setProjection(projectionNation1);

        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(1);

        ProjectionOperator projectionCustomer = new ProjectionOperator(new int[]{0,3});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension,
                TPCH_Schema.customer,
                _queryPlan).setHashIndexes(hashCustomer)
                           .setProjection(projectionCustomer);

        //-------------------------------------------------------------------------------------
        JoinComponent N_Cjoin = new JoinComponent(
                relationNation1,
                relationCustomer,
                _queryPlan).setProjection(new ProjectionOperator(new int[]{0, 2}))
                           .setHashIndexes(Arrays.asList(1));

        //-------------------------------------------------------------------------------------
        List<Integer> hashOrders = Arrays.asList(1);

        ProjectionOperator projectionOrders = new ProjectionOperator(new int[]{0,1});

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                TPCH_Schema.orders,
                _queryPlan).setHashIndexes(hashOrders)
                           .setProjection(projectionOrders);

        //-------------------------------------------------------------------------------------
        JoinComponent N_C_Ojoin = new JoinComponent(
                N_Cjoin,
                relationOrders,
                _queryPlan).setProjection(new ProjectionOperator(new int[]{0, 2}))
                           .setHashIndexes(Arrays.asList(1));

        //-------------------------------------------------------------------------------------
        List<Integer> hashSupplier = Arrays.asList(1);

        ProjectionOperator projectionSupplier = new ProjectionOperator(new int[]{0,3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                TPCH_Schema.supplier,
                _queryPlan).setHashIndexes(hashSupplier)
                           .setProjection(projectionSupplier);

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation2 = Arrays.asList(1);

        ProjectionOperator projectionNation2 = new ProjectionOperator(new int[]{1,0});

        DataSourceComponent relationNation2 = new DataSourceComponent(
                "NATION2",
                dataPath + "nation" + extension,
                TPCH_Schema.nation,
                _queryPlan).setHashIndexes(hashNation2)
                           .setSelection(selectionNation1)
                           .setProjection(projectionNation2);

        //-------------------------------------------------------------------------------------
        JoinComponent S_Njoin = new JoinComponent(
                relationSupplier,
                relationNation2,
                _queryPlan).setProjection(new ProjectionOperator(new int[]{0, 2}))
                           .setHashIndexes(Arrays.asList(0));

       //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(2);

        SelectionOperator selectionLineitem = new SelectionOperator(
                new BetweenPredicate(
                    new ColumnReference(_dateConv, 10),
                    true, new ValueSpecification(_dateConv, _date1),
                    true, new ValueSpecification(_dateConv, _date2)
                ));

        //first field in projection
        ValueExpression extractYear = new IntegerYearFromDate(
            new ColumnReference<Date>(_dateConv, 10));
        //second field in projection
            //1 - discount
        ValueExpression<Double> substract = new Subtraction(
                _doubleConv,
                new ValueSpecification(_doubleConv, 1.0),
                new ColumnReference(_doubleConv, 6));
            //extendedPrice*(1-discount)
        ValueExpression<Double> product = new Multiplication(
                _doubleConv,
                new ColumnReference(_doubleConv, 5),
                substract);
        //third field in projection
        ColumnReference supplierKey = new ColumnReference(_sc, 2);
        //forth field in projection
        ColumnReference orderKey = new ColumnReference(_sc, 0);
        ProjectionOperator projectionLineitem = new ProjectionOperator(extractYear, product, supplierKey, orderKey);

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                TPCH_Schema.lineitem,
                _queryPlan).setHashIndexes(hashLineitem)
                           .setSelection(selectionLineitem)
                           .setProjection(projectionLineitem);

        //-------------------------------------------------------------------------------------
        JoinComponent L_S_Njoin = new JoinComponent(
                relationLineitem,
                S_Njoin,
                _queryPlan).setProjection(new ProjectionOperator(new int[]{4, 0, 1, 3}))
                           .setHashIndexes(Arrays.asList(3));

        //-------------------------------------------------------------------------------------
        // set up aggregation function on the same StormComponent(Bolt) where the last join is
        SelectionOperator so = new SelectionOperator(
                new OrPredicate(
                    new AndPredicate(
                        new ComparisonPredicate(
                            new ColumnReference(_sc, 0),
                            new ValueSpecification(_sc, _firstCountryName)
                        ), new ComparisonPredicate(
                            new ColumnReference(_sc, 2),
                            new ValueSpecification(_sc, _secondCountryName)
                        )
                    ),
                    new AndPredicate(
                        new ComparisonPredicate(
                            new ColumnReference(_sc, 0),
                            new ValueSpecification(_sc, _secondCountryName)
                        ), new ComparisonPredicate(
                            new ColumnReference(_sc, 2),
                            new ValueSpecification(_sc, _firstCountryName)
                        )
                    )
                ));

        AggregateOperator agg = new AggregateSumOperator(_doubleConv, new ColumnReference(_doubleConv, 4), conf)
                .setGroupByColumns(Arrays.asList(0, 2, 3));

        JoinComponent N_C_O_L_S_Njoin = new JoinComponent(
                N_C_Ojoin,
                L_S_Njoin,
                _queryPlan).setSelection(so)
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