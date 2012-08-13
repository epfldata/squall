package plan_runner.query_plans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.ThetaJoinComponent;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
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

public class ThetaTPCH7Plan {
    private static Logger LOG = Logger.getLogger(ThetaTPCH7Plan.class);

    private QueryPlan _queryPlan = new QueryPlan();
    
    private static final IntegerConversion _ic = new IntegerConversion();

    private static final String    _date1Str = "1995-01-01";
    private static final String    _date2Str = "1996-12-31";
    private static final String  _firstCountryName = "FRANCE";
    private static final String _secondCountryName = "GERMANY";

    private static final TypeConversion<Date> _dateConv = new DateConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    private static final TypeConversion<String> _sc = new StringConversion();
    private static final Date _date1=_dateConv.fromString(_date1Str);
    private static final Date _date2=_dateConv.fromString(_date2Str);

    public ThetaTPCH7Plan(String dataPath, String extension, Map conf){

        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashNation1 = new ArrayList<Integer>(Arrays.asList(1));

        SelectOperator selectionNation1 = new SelectOperator(
                new OrPredicate(
                    new ComparisonPredicate(
                        new ColumnReference(_sc, 1),
                        new ValueSpecification(_sc, _firstCountryName)
                    ), new ComparisonPredicate(
                        new ColumnReference(_sc, 1),
                        new ValueSpecification(_sc, _secondCountryName)
                    )
                ));

        ProjectOperator projectionNation1 = new ProjectOperator(new int[]{1, 0});

        DataSourceComponent relationNation1 = new DataSourceComponent(
                "NATION1",
                dataPath + "nation" + extension,
                _queryPlan).setHashIndexes(hashNation1)
                           .addOperator(selectionNation1)
                           .addOperator(projectionNation1);

        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashCustomer = new ArrayList<Integer>(Arrays.asList(1));

        ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0,3});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension,
                _queryPlan).setHashIndexes(hashCustomer)
                           .addOperator(projectionCustomer);

        //-------------------------------------------------------------------------------------
        ColumnReference colN = new ColumnReference(_ic, 1);
	ColumnReference colC = new ColumnReference(_ic, 1);
	ComparisonPredicate N_C_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colN, colC);
				
        Component N_Cjoin = new ThetaJoinComponent(
                relationNation1,
                relationCustomer,
                _queryPlan).addOperator(new ProjectOperator(new int[]{0, 2}))
                           .setJoinPredicate(N_C_comp);

        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashOrders = new ArrayList<Integer>(Arrays.asList(1));

        ProjectOperator projectionOrders = new ProjectOperator(new int[]{0,1});

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                _queryPlan).setHashIndexes(hashOrders)
                           .addOperator(projectionOrders);

        //-------------------------------------------------------------------------------------
        
        ColumnReference colN_C = new ColumnReference(_ic, 1);
	ColumnReference colO = new ColumnReference(_ic, 1);
	ComparisonPredicate N_C_O_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colN_C, colO);
                
        Component N_C_Ojoin = new ThetaJoinComponent(
            N_Cjoin,
            relationOrders,
            _queryPlan).addOperator(new ProjectOperator(new int[]{0, 2}))
                       .setJoinPredicate(N_C_O_comp);
				 //.setHashIndexes(new ArrayList<Integer>(Arrays.asList(1)));	
        
        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashSupplier = new ArrayList<Integer>(Arrays.asList(1));

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0,3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                _queryPlan).setHashIndexes(hashSupplier)
                           .addOperator(projectionSupplier);

        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashNation2 = new ArrayList<Integer>(Arrays.asList(1));

        ProjectOperator projectionNation2 = new ProjectOperator(new int[]{1,0});

        DataSourceComponent relationNation2 = new DataSourceComponent(
                "NATION2",
                dataPath + "nation" + extension,
                _queryPlan).setHashIndexes(hashNation2)
                           .addOperator(selectionNation1)
                           .addOperator(projectionNation2);

        //-------------------------------------------------------------------------------------

        ColumnReference colS = new ColumnReference(_ic, 1);
	ColumnReference colN2 = new ColumnReference(_ic, 1);
	ComparisonPredicate S_N_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colS, colN2);
                
        Component S_Njoin = new ThetaJoinComponent(
                    relationSupplier,
                    relationNation2,
                    _queryPlan).addOperator(new ProjectOperator(new int[]{0, 2}))
                               .setJoinPredicate(S_N_comp);
                             //.setHashIndexes(new ArrayList<Integer>(Arrays.asList(0)));


       //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashLineitem = new ArrayList<Integer>(Arrays.asList(2));

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
                dataPath + "lineitem" + extension,
                _queryPlan).setHashIndexes(hashLineitem)
                           .addOperator(selectionLineitem)
                           .addOperator(projectionLineitem);

        //-------------------------------------------------------------------------------------
        
        ColumnReference colL = new ColumnReference(_ic, 2);
	ColumnReference colS_N = new ColumnReference(_ic, 0);
	ComparisonPredicate L_S_N_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colL, colS_N);
        
        Component L_S_Njoin = new ThetaJoinComponent(
            relationLineitem,
            S_Njoin,
            _queryPlan).addOperator(new ProjectOperator(new int[]{5, 0, 1, 3}))
                       .setJoinPredicate(L_S_N_comp);

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
                .setGroupByColumns(new ArrayList<Integer>(Arrays.asList(0, 2, 3)));
        
        ColumnReference colN_C_O = new ColumnReference(_ic, 1);
	ColumnReference colL_S_N = new ColumnReference(_ic, 3);
	ComparisonPredicate N_C_O_L_S_N_comp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, colN_C_O, colL_S_N);

                
        Component N_C_O_L_S_Njoin = new ThetaJoinComponent(
            N_C_Ojoin,
            L_S_Njoin,
            _queryPlan).addOperator(so)
                       .addOperator(agg).setJoinPredicate(N_C_O_L_S_N_comp);
        //-------------------------------------------------------------------------------------
        AggregateOperator overallAgg =
                    new AggregateSumOperator(new ColumnReference(_doubleConv, 1), conf)
                        .setGroupByColumns(Arrays.asList(0));

        _queryPlan.setOverallAggregation(overallAgg);

    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}