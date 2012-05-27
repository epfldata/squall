/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package queryPlans;

import components.DataSourceComponent;
import components.ThetaJoinComponent;
import conversion.IntegerConversion;
import expressions.ColumnReference;
import java.util.Map;

import org.apache.log4j.Logger;

import predicates.ComparisonPredicate;
import schema.TPCH_Schema;

public class TestThetaJoin {
    private static Logger LOG = Logger.getLogger(TestThetaJoin.class);

    private QueryPlan _queryPlan = new QueryPlan();
    private static final IntegerConversion _ic = new IntegerConversion();

    public TestThetaJoin(String dataPath, String extension, Map conf){
            //-------------------------------------------------------------------------------------
            DataSourceComponent relationNation = new DataSourceComponent(
                                            "NATION",
                                            dataPath + "nation" + extension,
                                            TPCH_Schema.nation,
                                            _queryPlan);

            //-------------------------------------------------------------------------------------
            DataSourceComponent relationRegion = new DataSourceComponent(
                                            "REGION",
                                            dataPath + "region" + extension,
                                            TPCH_Schema.region,
                                            _queryPlan);

            //-------------------------------------------------------------------------------------
            
            ColumnReference colNation = new ColumnReference(_ic, 2);
            ColumnReference colRegion = new ColumnReference(_ic, 0);
            ComparisonPredicate comp = new ComparisonPredicate(ComparisonPredicate.NONEQUAL_OP, colNation, colRegion);
            ThetaJoinComponent NATION_REGIONjoin = new ThetaJoinComponent(
                    relationNation,
                    relationRegion,
                    _queryPlan).setJoinPredicate(comp);

            //-------------------------------------------------------------------------------------
            
    }

    public QueryPlan getQueryPlan() {
        return _queryPlan;
    }
    
}