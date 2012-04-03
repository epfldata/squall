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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import org.apache.log4j.Logger;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;


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
        ArrayList<Integer> hashRegion = new ArrayList<Integer>(Arrays.asList(0));

        SelectionOperator selectionRegion = new SelectionOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 1),
                    new ValueSpecification(_sc, _region)
                ));
        
        ProjectionOperator projectionRegion = new ProjectionOperator(new int[]{0});

        DataSourceComponent relationRegion = new DataSourceComponent(
                "REGION",
                dataPath + "region" + extension,
                TPCH_Schema.region,
                _queryPlan).setHashIndexes(hashRegion)
                           .setSelection(selectionRegion)
                           .setProjection(projectionRegion);

        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashNation1 = new ArrayList<Integer>(Arrays.asList(1));

        ProjectionOperator projectionNation1 = new ProjectionOperator(new int[]{0,2});

        DataSourceComponent relationNation1 = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension,
                TPCH_Schema.nation,
                _queryPlan).setHashIndexes(hashNation1)
                           .setProjection(projectionNation1);

        //-------------------------------------------------------------------------------------
        JoinComponent R_Njoin = new JoinComponent(
                relationRegion,
                relationNation1,
                _queryPlan).setProjection(new ProjectionOperator(new int[]{1}))
                           .setHashIndexes(new ArrayList<Integer>(Arrays.asList(0)));
        
        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashCustomer = new ArrayList<Integer>(Arrays.asList(0));

        ProjectionOperator projectionCustomer = new ProjectionOperator(new int[]{3,0});

        DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER",
                dataPath + "customer" + extension,
                TPCH_Schema.customer,
                _queryPlan).setHashIndexes(hashCustomer)
                           .setProjection(projectionCustomer);

        //-------------------------------------------------------------------------------------
        JoinComponent R_N_Cjoin = new JoinComponent(
                R_Njoin,
                relationCustomer,
                _queryPlan).setProjection(new ProjectionOperator(new int[]{1}))
                           .setHashIndexes(new ArrayList<Integer>(Arrays.asList(0)));

        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashSupplier = new ArrayList<Integer>(Arrays.asList(1));

        ProjectionOperator projectionSupplier = new ProjectionOperator(new int[]{0, 3});

        DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER",
                dataPath + "supplier" + extension,
                TPCH_Schema.supplier,
                _queryPlan).setHashIndexes(hashSupplier)
                           .setProjection(projectionSupplier);

        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashNation2 = new ArrayList<Integer>(Arrays.asList(0));

        ProjectionOperator projectionNation2 = new ProjectionOperator(new int[]{0, 1});

        DataSourceComponent relationNation2 = new DataSourceComponent(
                "NATION",
                dataPath + "nation" + extension,
                TPCH_Schema.nation,
                _queryPlan).setHashIndexes(hashNation2)
                           .setProjection(projectionNation2);

        //-------------------------------------------------------------------------------------
        JoinComponent S_Njoin = new JoinComponent(
                relationSupplier,
                relationNation2,
                _queryPlan).setProjection(new ProjectionOperator(new int[]{0, 2}))
                           .setHashIndexes(new ArrayList<Integer>(Arrays.asList(0)));

        //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashPart = new ArrayList<Integer>(Arrays.asList(0));

        SelectionOperator selectionPart = new SelectionOperator(
                new ComparisonPredicate(
                    new ColumnReference(_sc, 4),
                    new ValueSpecification(_sc, _type)
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
        ArrayList<Integer> hashLineitem = new ArrayList<Integer>(Arrays.asList(1));

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
        ProjectionOperator projectionLineitem = new ProjectionOperator(orderKey, partKey, suppKey, product);

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
                _queryPlan).setProjection(new ProjectionOperator(new int[]{1,2,3}))
                           .setHashIndexes(new ArrayList<Integer>(Arrays.asList(0)));

       //-------------------------------------------------------------------------------------
        ArrayList<Integer> hashOrders = new ArrayList<Integer>(Arrays.asList(0));

        SelectionOperator selectionOrders = new SelectionOperator(
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
        ProjectionOperator projectionOrders = new ProjectionOperator(OrdersOrderKey, OrdersCustKey, OrdersExtractYear);

        DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS",
                dataPath + "orders" + extension,
                TPCH_Schema.orders,
                _queryPlan).setHashIndexes(hashOrders)
                           .setSelection(selectionOrders)
                           .setProjection(projectionOrders);

        //-------------------------------------------------------------------------------------
        JoinComponent P_L_Ojoin = new JoinComponent(
                P_Ljoin,
                relationOrders,
                _queryPlan).setProjection(new ProjectionOperator(new int[]{1,2,3,4}))
                           .setHashIndexes(new ArrayList<Integer>(Arrays.asList(0)));

        //-------------------------------------------------------------------------------------
        JoinComponent S_N_P_L_Ojoin = new JoinComponent(
                S_Njoin,
                P_L_Ojoin,
                _queryPlan).setProjection(new ProjectionOperator(new int[]{1,2,3,4}))
                           .setHashIndexes(new ArrayList<Integer>(Arrays.asList(2)));

        //-------------------------------------------------------------------------------------
        AggregateOperator agg = new AggregateSumOperator(_doubleConv, new ColumnReference(_doubleConv, 2), conf)
                .setGroupByColumns(new ArrayList<Integer>(Arrays.asList(1, 3)));

        JoinComponent R_N_C_S_N_P_L_Ojoin = new JoinComponent(
                R_N_Cjoin,
                S_N_P_L_Ojoin,
                _queryPlan).setAggregation(agg);
        
   }
   
   public QueryPlan getQueryPlan() {
    return _queryPlan;
   }
}