/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.JoinComponent;
import conversion.DoubleConversion;
import conversion.IntegerConversion;
import conversion.NumericConversion;
import expressions.ColumnReference;
import expressions.Multiplication;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.SelectionOperator;

import org.apache.log4j.Logger;
import predicates.ComparisonPredicate;
import schema.RST_Schema;

public class RSTPlan {
    private static Logger LOG = Logger.getLogger(RSTPlan.class);

    private static final NumericConversion<Double> _dc = new DoubleConversion();
    private static final NumericConversion<Integer> _ic = new IntegerConversion();
    
    private QueryPlan _queryPlan = new QueryPlan();

    public RSTPlan(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
                    // start of query plan filling
            ArrayList<Integer> hashR = new ArrayList<Integer>(Arrays.asList(1));

            DataSourceComponent relationR = new DataSourceComponent(
                                        "R",
                                        dataPath + "r" + extension,
                                        RST_Schema.R,
                                        _queryPlan).setHashIndexes(hashR);

            //-------------------------------------------------------------------------------------
            ArrayList<Integer> hashS = new ArrayList<Integer>(Arrays.asList(0));

            DataSourceComponent relationS = new DataSourceComponent(
                                            "S",
                                            dataPath + "s" + extension,
                                            RST_Schema.S,
                                            _queryPlan).setHashIndexes(hashS) ;

            //-------------------------------------------------------------------------------------
            ArrayList<Integer> hashIndexes = new ArrayList<Integer>(Arrays.asList(2));

            JoinComponent R_Sjoin = new JoinComponent(
                    relationR,
                    relationS,
                    _queryPlan).setHashIndexes(hashIndexes);
           
            //-------------------------------------------------------------------------------------
            ArrayList<Integer> hashT = new ArrayList<Integer>(Arrays.asList(0));

            DataSourceComponent relationT= new DataSourceComponent(
                                            "T",
                                            dataPath + "t" + extension,
                                            RST_Schema.T,
                                            _queryPlan).setHashIndexes(hashT);

            //-------------------------------------------------------------------------------------
            ValueExpression<Double> aggVe = new Multiplication(
                    _dc,
                    new ColumnReference(_dc, 0),
                    new ColumnReference(_dc, 3));

            AggregateSumOperator sp = new AggregateSumOperator(_dc, aggVe, conf);

            JoinComponent R_S_Tjoin= new JoinComponent(
                    R_Sjoin,
                    relationT,
                    _queryPlan).setAggregation(sp)
                               .setSelection(
                                    new SelectionOperator(
                                    new ComparisonPredicate(
                                        new ColumnReference(_ic, 1),
                                        new ValueSpecification(_ic, 10))));

            //-------------------------------------------------------------------------------------

            AggregateOperator overallAgg =
                    new AggregateSumOperator(_dc, new ColumnReference(_dc, 0), conf);

            _queryPlan.setOverallAggregation(overallAgg);
            
    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}