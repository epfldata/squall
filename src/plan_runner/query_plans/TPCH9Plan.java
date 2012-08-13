package plan_runner.query_plans;

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
import plan_runner.predicates.LikePredicate;

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

        SelectOperator selectionPart = new SelectOperator(
                new LikePredicate(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, COLOR)
                ));

        ProjectOperator projectionPart = new ProjectOperator(new int[]{0});

        DataSourceComponent relationPart = new DataSourceComponent(
                "PART",
                dataPath + "part" + extension,
                _queryPlan).setHashIndexes(hashPart)
                           .addOperator(selectionPart)
                           .addOperator(projectionPart);

        //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(1);

        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 1, 2, 4, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                _queryPlan).setHashIndexes(hashLineitem)
                           .addOperator(projectionLineitem);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent P_Ljoin = new EquiJoinComponent(
				relationPart,
				relationLineitem,
				_queryPlan).setHashIndexes(Arrays.asList(0, 2));
        //-------------------------------------------------------------------------------------

        List<Integer> hashPartsupp = Arrays.asList(0, 1);

        ProjectOperator projectionPartsupp = new ProjectOperator(new int[]{0, 1, 3});

        DataSourceComponent relationPartsupp = new DataSourceComponent(
                "PARTSUPP",
                dataPath + "partsupp" + extension,
                _queryPlan).setHashIndexes(hashPartsupp)
                           .addOperator(projectionPartsupp);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent P_L_PSjoin = new EquiJoinComponent(
				P_Ljoin,
				relationPartsupp,
				_queryPlan).setHashIndexes(Arrays.asList(0))
                                           .addOperator(new ProjectOperator(new int[]{1, 2, 3, 4, 5, 6}));
        //-------------------------------------------------------------------------------------

        List<Integer> hashOrders = Arrays.asList(0);

        ProjectOperator projectionOrders = new ProjectOperator(
                new ColumnReference(_sc, 0),
                new IntegerYearFromDate(new ColumnReference(_dateConv, 4)));

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                _queryPlan).setHashIndexes(hashOrders)
                           .addOperator(projectionOrders);

        //-------------------------------------------------------------------------------------

        EquiJoinComponent P_L_PS_Ojoin = new EquiJoinComponent(
				P_L_PSjoin,
				relationOrders,
				_queryPlan).setHashIndexes(Arrays.asList(0))
                                           .addOperator(new ProjectOperator(new int[]{1, 2, 3, 4, 5, 6}));
        //-------------------------------------------------------------------------------------

        List<Integer> hashSupplier = Arrays.asList(0);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                _queryPlan).setHashIndexes(hashSupplier)
                           .addOperator(projectionSupplier);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent P_L_PS_O_Sjoin = new EquiJoinComponent(
				P_L_PS_Ojoin,
				relationSupplier,
				_queryPlan).setHashIndexes(Arrays.asList(5))
                                           .addOperator(new ProjectOperator(new int[]{1, 2, 3, 4, 5, 6}));
        //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(0);

        ProjectOperator projectionNation = new ProjectOperator(new int[]{0, 1});

        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension,
                _queryPlan).setHashIndexes(hashNation)
                           .addOperator(projectionNation);

        //-------------------------------------------------------------------------------------
        // set up aggregation function on the StormComponent(Bolt) where join is performed

	//1 - discount
	ValueExpression<Double> substract1 = new Subtraction(
			new ValueSpecification(_doubleConv, 1.0),
			new ColumnReference(_doubleConv, 2));
	//extendedPrice*(1-discount)
	ValueExpression<Double> product1 = new Multiplication(
			new ColumnReference(_doubleConv, 1),
			substract1);

        //ps_supplycost * l_quantity
	ValueExpression<Double> product2 = new Multiplication(
			new ColumnReference(_doubleConv, 3),
			new ColumnReference(_doubleConv, 0));

        //all together
        ValueExpression<Double> substract2 = new Subtraction(
			product1,
			product2);

	AggregateOperator agg = new AggregateSumOperator(substract2, conf)
		.setGroupByColumns(Arrays.asList(5, 4));


        EquiJoinComponent P_L_PS_O_S_Njoin = new EquiJoinComponent(
				P_L_PS_O_Sjoin,
				relationNation,
				_queryPlan).addOperator(new ProjectOperator(new int[]{0, 1, 2, 3, 4, 6}))
                                           .addOperator(agg);
        //-------------------------------------------------------------------------------------

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}
