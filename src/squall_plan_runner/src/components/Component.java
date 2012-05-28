/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package components;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import expressions.ValueExpression;
import java.io.Serializable;
import java.util.List;
import operators.AggregateOperator;
import operators.DistinctOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import stormComponents.StormEmitter;
import stormComponents.synchronization.TopologyKiller;


public interface Component extends Serializable, StormEmitter {

    public void makeBolts(TopologyBuilder builder,
                       TopologyKiller killer,
                       List<String> allCompNames,
                       Config conf,
                       int partitioningType,
                       int hierarchyPosition);

    public String getName();
    public String getInfoID();
    public Component setPrintOut(boolean printOut);
    
    //sending the content of the component every 'millis' milliseconds
    public Component setBatchOutputMode(long millis);

    //this needs to be separatelly kept, due to Parser.SelectItemsVisitor.ComplexCondition
    //  in short, whether the component uses indexes or expressions
    //     is also dependent on on other component taking part in a join
    public Component setHashIndexes(List<Integer> hashIndexes);
    public List<Integer> getHashIndexes();
    public Component setHashExpressions(List<ValueExpression> hashExpressions);
    public List<ValueExpression> getHashExpressions();

    public Component setSelection(SelectionOperator selection);
    public SelectionOperator getSelection();
    public Component setDistinct(DistinctOperator distinct);
    public DistinctOperator getDistinct();
    public Component setProjection(ProjectionOperator projection);
    public ProjectionOperator getProjection();
    public Component setAggregation(AggregateOperator aggregation);
    public AggregateOperator getAggregation();


    // methods necessary for query plan processing
    public Component[] getParents();
    public void setChild(Component child);
    public Component getChild();
    public int getPreOpsOutputSize();
    public List<DataSourceComponent> getAncestorDataSources();

    //method necessary for direct grouping and load balancing:
    //at receiver side:
    public Component setFullHashList(List<String> fullHashList);
    public List<String> getFullHashList();
}
