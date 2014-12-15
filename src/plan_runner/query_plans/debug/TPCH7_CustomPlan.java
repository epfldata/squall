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
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.IntegerYearFromDate;
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
import plan_runner.predicates.OrPredicate;
import plan_runner.query_plans.QueryBuilder;

public class TPCH7_CustomPlan {
    private static Logger LOG = Logger.getLogger(TPCH7_CustomPlan.class);

    private QueryBuilder _queryBuilder = new QueryBuilder();

	private static final String    _date1Str = "1995-01-01";
	private static final String    _date2Str = "1996-12-31";
//	private static final String  _firstCountryName = "FRANCE";
//	private static final String _secondCountryName = "GERMANY";
	private static final String  _firstCountryName = "PERU";
	private static final String _secondCountryName = "ETHIOPIA";

	private static final TypeConversion<Date> _dateConv = new DateConversion();
	private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
	private static final TypeConversion<String> _sc = new StringConversion();
	private static final Date _date1=_dateConv.fromString(_date1Str);
	private static final Date _date2=_dateConv.fromString(_date2Str);

    public TPCH7_CustomPlan(String dataPath, String extension, Map conf){

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation2 = Arrays.asList(1);

        SelectOperator selectionNation2 = new SelectOperator(
                new OrPredicate(
                    new ComparisonPredicate(
                        new ColumnReference(_sc, 1),
                        new ValueSpecification(_sc, _firstCountryName)
                    ), new ComparisonPredicate(
                        new ColumnReference(_sc, 1),
                        new ValueSpecification(_sc, _secondCountryName)
                    )
                ));

        ProjectOperator projectionNation2 = new ProjectOperator(new int[]{1, 0});

        DataSourceComponent relationNation2 = new DataSourceComponent(
                "NATION2",
                dataPath + "nation" + extension).setHashIndexes(hashNation2)
                           .addOperator(selectionNation2)
                           .addOperator(projectionNation2);
        _queryBuilder.add(relationNation2);

        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(1);

        ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0,3});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension).setHashIndexes(hashCustomer)
                           .addOperator(projectionCustomer);
        _queryBuilder.add(relationCustomer);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent N_Cjoin = new EquiJoinComponent(
                relationNation2,
                relationCustomer).addOperator(new ProjectOperator(new int[]{0, 2}))
                           .setHashIndexes(Arrays.asList(1));
        _queryBuilder.add(N_Cjoin);

        //-------------------------------------------------------------------------------------
        List<Integer> hashOrders = Arrays.asList(1);

        ProjectOperator projectionOrders = new ProjectOperator(new int[]{0,1});

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension).setHashIndexes(hashOrders)
                           .addOperator(projectionOrders);
        _queryBuilder.add(relationOrders);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent N_C_Ojoin = new EquiJoinComponent(
                N_Cjoin,
                relationOrders).addOperator(new ProjectOperator(new int[]{0, 2}))
                           .setHashIndexes(Arrays.asList(1));
        _queryBuilder.add(N_C_Ojoin);

        //-------------------------------------------------------------------------------------
        List<Integer> hashSupplier = Arrays.asList(1);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0,3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension).setHashIndexes(hashSupplier)
                           .addOperator(projectionSupplier);
        _queryBuilder.add(relationSupplier);

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation1 = Arrays.asList(1);

        ProjectOperator projectionNation1 = new ProjectOperator(new int[]{1,0});

        DataSourceComponent relationNation1 = new DataSourceComponent(
                "NATION1",
                dataPath + "nation" + extension).setHashIndexes(hashNation1)
                           .addOperator(selectionNation2)
                           .addOperator(projectionNation1);
        _queryBuilder.add(relationNation1);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent S_Njoin = new EquiJoinComponent(
                relationSupplier,
                relationNation1).addOperator(new ProjectOperator(new int[]{0, 2}))
                           .setHashIndexes(Arrays.asList(0));
        _queryBuilder.add(S_Njoin);

       //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(2);

        SelectOperator selectionLineitem = new SelectOperator(
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
                new ValueSpecification(_doubleConv, 1.0),
                new ColumnReference(_doubleConv, 6));
            //extendedPrice*(1-discount)
        ValueExpression<Double> product = new Multiplication(
                new ColumnReference(_doubleConv, 5),
                substract);
        //third field in projection
        ColumnReference supplierKey = new ColumnReference(_sc, 2);
        //forth field in projection
        ColumnReference orderKey = new ColumnReference(_sc, 0);
        ProjectOperator projectionLineitem = new ProjectOperator(extractYear, product, supplierKey, orderKey);

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension).setHashIndexes(hashLineitem)
                           .addOperator(selectionLineitem)
                           .addOperator(projectionLineitem);
        _queryBuilder.add(relationLineitem);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent L_S_Njoin = new EquiJoinComponent(
                relationLineitem,
                S_Njoin).addOperator(new ProjectOperator(new int[]{4, 0, 1, 3}))
                           .setHashIndexes(Arrays.asList(3))
                           ;
        _queryBuilder.add(L_S_Njoin);

        //-------------------------------------------------------------------------------------     
        // set up aggregation function on the same StormComponent(Bolt) where the last join is
        SelectOperator so = new SelectOperator(
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

        AggregateOperator agg = new AggregateSumOperator(new ColumnReference(_doubleConv, 4), conf)
                .setGroupByColumns(Arrays.asList(2, 0, 3));

        EquiJoinComponent N_C_O_L_S_Njoin = new EquiJoinComponent(
                N_C_Ojoin,
                L_S_Njoin).addOperator(so)
                           .addOperator(agg);
        _queryBuilder.add(N_C_O_L_S_Njoin);
        //-------------------------------------------------------------------------------------

    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}