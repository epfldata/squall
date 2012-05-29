/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.EquiJoinComponent;
import conversion.DoubleConversion;
import conversion.IntegerConversion;
import conversion.NumericConversion;
import expressions.ColumnReference;
import expressions.Multiplication;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.SelectOperator;

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
            List<Integer> hashR = Arrays.asList(1);

            DataSourceComponent relationR = new DataSourceComponent(
                                        "R",
                                        dataPath + "r" + extension,
                                        RST_Schema.R,
                                        _queryPlan).setHashIndexes(hashR);

            //-------------------------------------------------------------------------------------
            List<Integer> hashS = Arrays.asList(0);

            DataSourceComponent relationS = new DataSourceComponent(
                                            "S",
                                            dataPath + "s" + extension,
                                            RST_Schema.S,
                                            _queryPlan).setHashIndexes(hashS) ;

            //-------------------------------------------------------------------------------------
            List<Integer> hashIndexes = Arrays.asList(2);

            EquiJoinComponent R_Sjoin = new EquiJoinComponent(
                    relationR,
                    relationS,
                    _queryPlan).setHashIndexes(hashIndexes);
           
            //-------------------------------------------------------------------------------------
            List<Integer> hashT = Arrays.asList(0);

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

            EquiJoinComponent R_S_Tjoin= new EquiJoinComponent(
                    R_Sjoin,
                    relationT,
                    _queryPlan).addOperator(
                                    new SelectOperator(
                                    new ComparisonPredicate(
                                        new ColumnReference(_ic, 1),
                                        new ValueSpecification(_ic, 10))))
                               .addOperator(sp);
            //-------------------------------------------------------------------------------------

            AggregateOperator overallAgg =
                    new AggregateSumOperator(_dc, new ColumnReference(_dc, 0), conf);

            _queryPlan.setOverallAggregation(overallAgg);
            
    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
}