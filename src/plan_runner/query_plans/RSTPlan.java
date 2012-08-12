package plan_runner.query_plans;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.conversion.DoubleConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.NumericConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.AggregateSumOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.predicates.ComparisonPredicate;

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
                                        _queryPlan).setHashIndexes(hashR);

            //-------------------------------------------------------------------------------------
            List<Integer> hashS = Arrays.asList(0);

            DataSourceComponent relationS = new DataSourceComponent(
                                            "S",
                                            dataPath + "s" + extension,
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