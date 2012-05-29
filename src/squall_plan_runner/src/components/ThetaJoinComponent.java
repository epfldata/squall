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
import stormComponents.StormThetaJoin;
import stormComponents.synchronization.TopologyKiller;
import org.apache.log4j.Logger;

import predicates.Predicate;
import queryPlans.QueryPlan;
import stormComponents.StormComponent;
import utilities.MyUtilities;

public class ThetaJoinComponent implements Component {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(ThetaJoinComponent.class);

    private Component _firstParent;
    private Component _secondParent;
    private Component _child;

    private String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private StormThetaJoin _joiner;

    private ChainOperator _chain = new ChainOperator();

    private boolean _printOut;
    private boolean _printOutSet; //whether printOut was already set
    
    private Predicate _joinPredicate;

    public ThetaJoinComponent(Component firstParent,
                    Component secondParent,
                    QueryPlan queryPlan){
      _firstParent = firstParent;
      _firstParent.setChild(this);
      _secondParent = secondParent;
      _secondParent.setChild(this);

      _componentName = firstParent.getName() + "_" + secondParent.getName();

      queryPlan.add(this);
    }
    
    public ThetaJoinComponent setJoinPredicate(Predicate joinPredicate) {
    	_joinPredicate = joinPredicate;
    	return this;
    }
    
    public Predicate getJoinPredicate() {
    	return _joinPredicate;
    }

    //list of distinct keys, used for direct stream grouping and load-balancing ()
    @Override
    public ThetaJoinComponent setFullHashList(List<String> fullHashList){
        throw new RuntimeException("Load balancing for Theta join is done inherently!");
    }

    @Override
    public List<String> getFullHashList(){
        throw new RuntimeException("Load balancing for Theta join is done inherently!");
    }

    @Override
    public ThetaJoinComponent setHashIndexes(List<Integer> hashIndexes){
        _hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public ThetaJoinComponent setHashExpressions(List<ValueExpression> hashExpressions){
        _hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public ThetaJoinComponent addOperator(Operator operator){
	_chain.addOperator(operator);
        return this;
    }

    @Override
    public ChainOperator getChainOperator(){
        return _chain;
    }


    @Override
    public ThetaJoinComponent setPrintOut(boolean printOut){
        _printOutSet = true;
        _printOut = printOut;
        return this;
    }

    @Override
    public ThetaJoinComponent setBatchOutputMode(long millis){
        _batchOutputMillis = millis;
        return this;
    }

    @Override
    public void makeBolts(TopologyBuilder builder,
            TopologyKiller killer,
            List<String> allCompNames,
            Config conf,
            int partitioningType,
            int hierarchyPosition){

        //by default print out for the last component
        //for other conditions, can be set via setPrintOut
        if(hierarchyPosition==StormComponent.FINAL_COMPONENT && !_printOutSet){
           setPrintOut(true);
        }

        MyUtilities.checkBatchOutput(_batchOutputMillis, _chain.getAggregation(), conf);

        _joiner = new StormThetaJoin(_firstParent,
                            _secondParent,
                            _componentName,
                            allCompNames,
                            _chain,
                            _hashIndexes,
                            _hashExpressions,
                            _joinPredicate,
                            hierarchyPosition,
                            _printOut,
                            _batchOutputMillis,
                            builder,
                            killer,
                            conf);   
    }

    @Override
    public Component[] getParents() {
        return new Component[]{_firstParent, _secondParent};
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
    public int getPreOpsOutputSize(){
        int joinColumnsLength = _firstParent.getHashIndexes().size();
        return _firstParent.getPreOpsOutputSize() + _secondParent.getPreOpsOutputSize() - joinColumnsLength;
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources(){
        List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
        for(Component parent: getParents()){
            list.addAll(parent.getAncestorDataSources());
        }
        return list;
    }

    // from StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
         return _joiner.getEmitterIDs();
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
        return _joiner.getInfoID();
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
        int hash = 7;
        hash = 37 * hash + (this._componentName != null ? this._componentName.hashCode() : 0);
        return hash;
    }

}