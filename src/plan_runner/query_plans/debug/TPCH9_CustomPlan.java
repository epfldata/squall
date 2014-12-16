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
import plan_runner.predicates.LikePredicate;
import plan_runner.query_plans.QueryBuilder;

public class TPCH9_CustomPlan {
    private static Logger LOG = Logger.getLogger(TPCH9_CustomPlan.class);

    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    private static final TypeConversion<Date> _dateConv = new DateConversion();
    private static final StringConversion _sc = new StringConversion();
    
    private static final String COLOR = "%green%";

    private QueryBuilder _queryBuilder = new QueryBuilder();

    public TPCH9_CustomPlan(String dataPath, String extension, Map conf){
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
                dataPath + "part" + extension).setOutputPartKey(hashPart)
//                           .addOperator(selectionPart)
                           .add(projectionPart);
        _queryBuilder.add(relationPart);

        //-------------------------------------------------------------------------------------
        List<Integer> hashLineitem = Arrays.asList(1);

        ProjectOperator projectionLineitem = new ProjectOperator(new int[]{0, 1, 2, 4, 5, 6});

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension).setOutputPartKey(hashLineitem)
                           .add(projectionLineitem);
        _queryBuilder.add(relationLineitem);
        
        //-------------------------------------------------------------------------------------
        EquiJoinComponent P_Ljoin = new EquiJoinComponent(
				relationPart,
				relationLineitem).setOutputPartKey(Arrays.asList(0, 2))
				;
        _queryBuilder.add(P_Ljoin);
        //-------------------------------------------------------------------------------------
      
        List<Integer> hashPartsupp = Arrays.asList(0, 1);

        ProjectOperator projectionPartsupp = new ProjectOperator(new int[]{0, 1, 3});

        DataSourceComponent relationPartsupp = new DataSourceComponent(
                "PARTSUPP",
                dataPath + "partsupp" + extension).setOutputPartKey(hashPartsupp)
                           .add(projectionPartsupp);
        _queryBuilder.add(relationPartsupp);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent P_L_PSjoin = new EquiJoinComponent(
				P_Ljoin,
				relationPartsupp).setOutputPartKey(Arrays.asList(0))
                                           .add(new ProjectOperator(new int[]{1, 2, 3, 4, 5, 6}));
        _queryBuilder.add(P_L_PSjoin);
        //-------------------------------------------------------------------------------------

        List<Integer> hashOrders = Arrays.asList(0);

        ProjectOperator projectionOrders = new ProjectOperator(
                new ColumnReference(_sc, 0),
                new IntegerYearFromDate(new ColumnReference(_dateConv, 4)));

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension).setOutputPartKey(hashOrders)
                           .add(projectionOrders);
        _queryBuilder.add(relationOrders);

        //-------------------------------------------------------------------------------------

        EquiJoinComponent P_L_PS_Ojoin = new EquiJoinComponent(
				P_L_PSjoin,
				relationOrders).setOutputPartKey(Arrays.asList(0))
                                           .add(new ProjectOperator(new int[]{1, 2, 3, 4, 5, 6}));
        _queryBuilder.add(P_L_PS_Ojoin);
        //-------------------------------------------------------------------------------------

        List<Integer> hashSupplier = Arrays.asList(0);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension).setOutputPartKey(hashSupplier)
                           .add(projectionSupplier);
        _queryBuilder.add(relationSupplier);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent P_L_PS_O_Sjoin = new EquiJoinComponent(
				P_L_PS_Ojoin,
				relationSupplier).setOutputPartKey(Arrays.asList(5))
                                           .add(new ProjectOperator(new int[]{1, 2, 3, 4, 5, 6}));
        _queryBuilder.add(P_L_PS_O_Sjoin);
        //-------------------------------------------------------------------------------------
        List<Integer> hashNation = Arrays.asList(0);

        ProjectOperator projectionNation = new ProjectOperator(new int[]{0, 1});

        DataSourceComponent relationNation = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension).setOutputPartKey(hashNation)
                           .add(projectionNation);
        _queryBuilder.add(relationNation);

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
				relationNation).add(new ProjectOperator(new int[]{0, 1, 2, 3, 4, 6}))
                                           .add(agg)
                                           ;
        _queryBuilder.add(P_L_PS_O_S_Njoin);
        //-------------------------------------------------------------------------------------

    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
