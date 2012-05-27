/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package components;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import operators.AggregateOperator;
import operators.DistinctOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import stormComponents.synchronization.TopologyKiller;

import org.apache.log4j.Logger;
import queryPlans.QueryPlan;
import stormComponents.StormComponent;
import stormComponents.StormOperator;
import utilities.MyUtilities;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

public  class OperatorComponent implements Component{
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(OperatorComponent.class);

    private String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private SelectionOperator _selection;
    private DistinctOperator _distinct;
    private ProjectionOperator _projection;
    private AggregateOperator _aggregation;

    private boolean _printOut;
    private boolean _printOutSet;

    private Component _parent;
    private Component _child;
    private StormOperator _stormOperator;

    private List<String> _fullHashList;

    public OperatorComponent(Component parent,
            String componentName,
            QueryPlan queryPlan){

        _parent = parent;
        _parent.setChild(this);

        _componentName = componentName;

        queryPlan.add(this);
    }

    @Override
    public OperatorComponent setFullHashList(List<String> fullHashList){
        _fullHashList = fullHashList;
        return this;
    }

    @Override
    public List<String> getFullHashList(){
        return _fullHashList;
    }

    @Override
     public OperatorComponent setHashIndexes(List<Integer> hashIndexes){
        _hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public OperatorComponent setHashExpressions(List<ValueExpression> hashExpressions){
        _hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public OperatorComponent setSelection(SelectionOperator selection){
        _selection = selection;
        return this;
    }

    @Override
    public OperatorComponent setDistinct(DistinctOperator distinct){
        _distinct = distinct;
        return this;
    }

    @Override
    public OperatorComponent setProjection(ProjectionOperator projection){
        _projection = projection;
        return this;
    }

    @Override
    public OperatorComponent setAggregation(AggregateOperator aggregation){
        _aggregation = aggregation;
        return this;
    }

    @Override
    public SelectionOperator getSelection() {
        return _selection;
    }

    @Override
    public DistinctOperator getDistinct() {
        return _distinct;
    }

    @Override
    public ProjectionOperator getProjection() {
        return _projection;
    }

    @Override
    public AggregateOperator getAggregation() {
        return _aggregation;
    }

    @Override
    public OperatorComponent setPrintOut(boolean printOut){
        _printOutSet = true;
        _printOut = printOut;
        return this;
    }

    @Override
    public OperatorComponent setBatchOutputMode(long millis){
        _batchOutputMillis = millis;
        return this;
    }

    @Override
    public void makeBolts(TopologyBuilder builder,
            TopologyKiller killer,
            Config conf,
            int partitioningType,
            int hierarchyPosition) {

        //by default print out for the last component
        //for other conditions, can be set via setPrintOut
        if(hierarchyPosition==StormComponent.FINAL_COMPONENT && !_printOutSet){
            setPrintOut(true);
        }

        MyUtilities.checkBatchOutput(_batchOutputMillis, _aggregation, conf);

        _stormOperator = new StormOperator(_parent,
                _componentName,
                _selection,
                _distinct,
                _projection,
                _aggregation,
                _hashIndexes,
                _hashExpressions,
                hierarchyPosition,
                _printOut,
                _batchOutputMillis,
                _fullHashList,
                builder,
                killer,
                conf);
    }

    @Override
    public Component[] getParents() {
        return new Component[]{_parent};
    }

    @Override
    public Component getChild() {
        return _child;
    }

    @Override
    public void setChild(Component child) {
        _child = child;
    }

    @Override
    public int getOutputSize(){
        return _parent.getOutputSize();
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources(){
        List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
        list.addAll(_parent.getAncestorDataSources());
        return list;
    }

    // from StormComponent
    @Override
    public int[] getEmitterIDs() {
        return _stormOperator.getEmitterIDs();
    }

    @Override
    public String getName() {
        return _componentName;
    }

    @Override
    public List<Integer> getHashIndexes() {
        return _hashIndexes;
    }

    @Override
    public List<ValueExpression> getHashExpressions() {
        return _hashExpressions;
    }

    @Override
    public String getInfoID() {
        return _stormOperator.getInfoID();
    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof Component){
            return _componentName.equals(((Component)obj).getName());
        }else{
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 47 * hash + (this._componentName != null ? this._componentName.hashCode() : 0);
        return hash;
    }

}