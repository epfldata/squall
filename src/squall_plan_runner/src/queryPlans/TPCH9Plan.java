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
import predicates.LikePredicate;

public class TPCH9Plan {
    private static Logger LOG = Logger.getLogger(TPCH9Plan.class);

    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    private static final TypeConversion<Date> _dateConv = new DateConversion();
    private static final StringConversion _sc = new StringConversion();
    
    private static final String COLOR = "%green%";

    private QueryPlan _queryPlan = new QueryPlan();

    public TPCH9Plan(String dataPath, String extension, Map conf){
        //-------------------------------------------------------------------------------------
        List<Integer> hashPart = Arrays.asList(0);

        SelectionOperator selectionPart = new SelectionOperator(
                new LikePredicate(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, COLOR)
                ));

        ProjectionOperator projectionPart = new ProjectionOperator(new int[]{0});

        DataSourceComponent relationPart = new DataSourceComponent(
                "PART",
                dataPath + "part" + extension,
                TPCH_Schema.part,
                _queryPlan).setHashIndexes(hashPart)
                           .setSelection(selectionPart)
                           .setProjection(projectionPart);

        //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(1);

        ProjectionOperator projectionLineitem = new ProjectionOperator(new int[]{0, 1, 2, 4, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                TPCH_Schema.lineitem,
                _queryPlan).setHashIndexes(hashLineitem)
                           .setProjection(projectionLineitem);

        //-------------------------------------------------------------------------------------
        JoinComponent P_Ljoin = new JoinComponent(
				relationPart,
				relationLineitem,
				_queryPlan).setHashIndexes(Arrays.asList(0, 2));
        //-------------------------------------------------------------------------------------

        List<Integer> hashPartsupp = Arrays.asList(0, 1);

        ProjectionOperator projectionPartsupp = new ProjectionOperator(new int[]{0, 1, 3});

        DataSourceComponent relationPartsupp = new DataSourceComponent(
                "PARTSUPP",
                dataPath + "partsupp" + extension,
                TPCH_Schema.partsupp,
                _queryPlan).setHashIndexes(hashPartsupp)
                           .setProjection(projectionPartsupp);

        //-------------------------------------------------------------------------------------
        JoinComponent P_L_PSjoin = new JoinComponent(
				P_Ljoin,
				relationPartsupp,
				_queryPlan).setHashIndexes(Arrays.asList(0))
                                           .setProjection(new ProjectionOperator(new int[]{1, 2, 3, 4, 5, 6}));
        //-------------------------------------------------------------------------------------

        List<Integer> hashOrders = Arrays.asList(0);

        ProjectionOperator projectionOrders = new ProjectionOperator(
                new ColumnReference(_sc, 0),
                new IntegerYearFromDate(new ColumnReference(_dateConv, 4)));

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                TPCH_Schema.orders,
                _queryPlan).setHashIndexes(hashOrders)
                           .setProjection(projectionOrders);

        //-------------------------------------------------------------------------------------

        JoinComponent P_L_PS_Ojoin = new JoinComponent(
				P_L_PSjoin,
				relationOrders,
				_queryPlan).setHashIndexes(Arrays.asList(0))
                                           .setProjection(new ProjectionOperator(new int[]{1, 2, 3, 4, 5, 6}));
        //-------------------------------------------------------------------------------------

        List<Integer> hashSupplier = Arrays.asList(0);

        ProjectionOperator projectionSupplier = new ProjectionOperator(new int[]{0, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                TPCH_Schema.supplier,
                _queryPlan).setHashIndexes(hashSupplier)
                           .setProjection(projectionSupplier);

        //-------------------------------------------------------------------------------------
        JoinComponent P_L_PS_O_Sjoin = new JoinComponent(
				P_L_PS_Ojoin,
				relationSupplier,
				_queryPlan).setHashIndexes(Arrays.asList(5))
                                           .setProjection(new ProjectionOperator(new int[]{1, 2, 3, 4, 5, 6}));
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
        // set up aggregation function on the StormComponent(Bolt) where join is performed

	//1 - discount
	ValueExpression<Double> substract1 = new Subtraction(
			_doubleConv,
			new ValueSpecification(_doubleConv, 1.0),
			new ColumnReference(_doubleConv, 2));
	//extendedPrice*(1-discount)
	ValueExpression<Double> product1 = new Multiplication(
			_doubleConv,
			new ColumnReference(_doubleConv, 1),
			substract1);

        //ps_supplycost * l_quantity
	ValueExpression<Double> product2 = new Multiplication(
			_doubleConv,
			new ColumnReference(_doubleConv, 3),
			new ColumnReference(_doubleConv, 0));

        //all together
        ValueExpression<Double> substract2 = new Subtraction(
			_doubleConv,
			product1,
			product2);

	AggregateOperator agg = new AggregateSumOperator(_doubleConv, substract2, conf)
		.setGroupByColumns(Arrays.asList(5, 4));


        JoinComponent P_L_PS_O_S_Njoin = new JoinComponent(
				P_L_PS_O_Sjoin,
				relationNation,
				_queryPlan).setProjection(new ProjectionOperator(new int[]{0, 1, 2, 3, 4, 6}))
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
