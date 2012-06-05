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
import operators.ChainOperator;
import operators.Operator;
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

    private ChainOperator _chain = new ChainOperator();

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
    public OperatorComponent addOperator(Operator operator){
	_chain.addOperator(operator);
        return this;
    }

    @Override
    public ChainOperator getChainOperator(){
        return _chain;
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
            List<String> allCompNames,
            Config conf,
            int partitioningType,
            int hierarchyPosition) {

        //by default print out for the last component
        //for other conditions, can be set via setPrintOut
        if(hierarchyPosition==StormComponent.FINAL_COMPONENT && !_printOutSet){
            setPrintOut(true);
        }

        MyUtilities.checkBatchOutput(_batchOutputMillis, _chain.getAggregation(), conf);

        _stormOperator = new StormOperator(_parent,
                _componentName,
                allCompNames,
                _chain,
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
    public List<DataSourceComponent> getAncestorDataSources(){
        List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
        list.addAll(_parent.getAncestorDataSources());
        return list;
    }

    // from StormComponent
    @Override
    public String[] getEmitterIDs() {
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