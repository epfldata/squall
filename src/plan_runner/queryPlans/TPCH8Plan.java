/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.queryPlans;

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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import org.apache.log4j.Logger;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;


public class TPCH8Plan {
    private static Logger LOG = Logger.getLogger(TPCH8Plan.class);

    private QueryPlan _queryPlan = new QueryPlan();

   // the field nation is not used, since we cannot provide final result if having more final components
   private static final String _nation="BRAZIL";
   private static final String _region="AMERICA";
   private static final String _type="ECONOMY ANODIZED STEEL";
   private static final String _date1Str = "1995-01-01";
   private static final String _date2Str = "1996-12-31";

   private static final TypeConversion<Date> _dateConv = new DateConversion();
   private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
   private static final TypeConversion<String> _sc = new StringConversion();

   private static final Date _date1=_dateConv.fromString(_date1Str);
   private static final Date _date2=_dateConv.fromString(_date2Str);


   public TPCH8Plan(String dataPath, String extension, Map conf){
        //-------------------------------------------------------------------------------------
        List<Integer> hashRegion = Arrays.asList(0);

        SelectOperator selectionRegion = new SelectOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, _region)
                ));
        
        ProjectOperator projectionRegion = new ProjectOperator(new int[]{0});

        DataSourceComponent relationRegion = new DataSourceComponent(
                "REGION",
                dataPath + "region" + extension,
                _queryPlan).setHashIndexes(hashRegion)
                           .addOperator(selectionRegion)
                           .addOperator(projectionRegion);

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation1 = Arrays.asList(1);

        ProjectOperator projectionNation1 = new ProjectOperator(new int[]{0,2});

        DataSourceComponent relationNation1 = new DataSourceComponent(
                "NATION1",
                dataPath + "nation" + extension,
                _queryPlan).setHashIndexes(hashNation1)
                           .addOperator(projectionNation1);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent R_Njoin = new EquiJoinComponent(
                relationRegion,
                relationNation1,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1}))
                           .setHashIndexes(Arrays.asList(0));
        
        //-------------------------------------------------------------------------------------
        List<Integer> hashCustomer = Arrays.asList(0);

        ProjectOperator projectionCustomer = new ProjectOperator(new int[]{3,0});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension,
                _queryPlan).setHashIndexes(hashCustomer)
                           .addOperator(projectionCustomer);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent R_N_Cjoin = new EquiJoinComponent(
                R_Njoin,
                relationCustomer,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1}))
                           .setHashIndexes(Arrays.asList(0));

        //-------------------------------------------------------------------------------------
        List<Integer> hashSupplier = Arrays.asList(1);

        ProjectOperator projectionSupplier = new ProjectOperator(new int[]{0, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                _queryPlan).setHashIndexes(hashSupplier)
                           .addOperator(projectionSupplier);

        //-------------------------------------------------------------------------------------
        List<Integer> hashNation2 = Arrays.asList(0);

        ProjectOperator projectionNation2 = new ProjectOperator(new int[]{0, 1});

        DataSourceComponent relationNation2 = new DataSourceComponent(
                "NATION2",
                dataPath + "nation" + extension,
                _queryPlan).setHashIndexes(hashNation2)
                           .addOperator(projectionNation2);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent S_Njoin = new EquiJoinComponent(
                relationSupplier,
                relationNation2,
                _queryPlan).addOperator(new ProjectOperator(new int[]{0, 2}))
                           .setHashIndexes(Arrays.asList(0));

        //-------------------------------------------------------------------------------------
        List<Integer> hashPart = Arrays.asList(0);

        SelectOperator selectionPart = new SelectOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 4),
                    new ValueSpecification(_sc, _type)
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

        //first field in projection
        ColumnReference orderKey = new ColumnReference(_sc, 0);
        //second field in projection
        ColumnReference partKey = new ColumnReference(_sc, 1);
        //third field in projection
        ColumnReference suppKey = new ColumnReference(_sc, 2);
        //forth field in projection
        ValueExpression<Double> substract = new Subtraction(
                _doubleConv,
                new ValueSpecification(_doubleConv, 1.0),
                new ColumnReference(_doubleConv, 6));
            //extendedPrice*(1-discount)
        ValueExpression<Double> product = new Multiplication(
                _doubleConv,
                new ColumnReference(_doubleConv, 5),
                substract);
        ProjectOperator projectionLineitem = new ProjectOperator(orderKey, partKey, suppKey, product);

        DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM",
                dataPath + "lineitem" + extension,
                _queryPlan).setHashIndexes(hashLineitem)
                           .addOperator(projectionLineitem);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent P_Ljoin = new EquiJoinComponent(
                relationPart,
                relationLineitem,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1,2,3}))
                           .setHashIndexes(Arrays.asList(0));

       //-------------------------------------------------------------------------------------
        List<Integer> hashOrders = Arrays.asList(0);

        SelectOperator selectionOrders = new SelectOperator(
                new BetweenPredicate(
                    new ColumnReference(_dateConv, 4),
                    true, new ValueSpecification(_dateConv, _date1),
                    true, new ValueSpecification(_dateConv, _date2)
                ));

        //first field in projection
        ValueExpression OrdersOrderKey = new ColumnReference(_sc, 0);
        //second field in projection
        ValueExpression OrdersCustKey = new ColumnReference(_sc, 1);
         //third field in projection
        ValueExpression OrdersExtractYear = new IntegerYearFromDate(
            new ColumnReference<Date>(_dateConv, 4));
        ProjectOperator projectionOrders = new ProjectOperator(OrdersOrderKey, OrdersCustKey, OrdersExtractYear);

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                _queryPlan).setHashIndexes(hashOrders)
                           .addOperator(selectionOrders)
                           .addOperator(projectionOrders);

        //-------------------------------------------------------------------------------------
        EquiJoinComponent P_L_Ojoin = new EquiJoinComponent(
                P_Ljoin,
                relationOrders,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1,2,3,4}))
                           .setHashIndexes(Arrays.asList(0));

        //-------------------------------------------------------------------------------------
        EquiJoinComponent S_N_P_L_Ojoin = new EquiJoinComponent(
                S_Njoin,
                P_L_Ojoin,
                _queryPlan).addOperator(new ProjectOperator(new int[]{1,2,3,4}))
                           .setHashIndexes(Arrays.asList(2));

        //-------------------------------------------------------------------------------------
        AggregateOperator agg = new AggregateSumOperator(_doubleConv, new ColumnReference(_doubleConv, 2), conf)
                .setGroupByColumns(Arrays.asList(1, 3));

        EquiJoinComponent R_N_C_S_N_P_L_Ojoin = new EquiJoinComponent(
                R_N_Cjoin,
                S_N_P_L_Ojoin,
                _queryPlan).addOperator(agg);

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