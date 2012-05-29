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
import operators.ChainOperator;
import operators.DistinctOperator;
import operators.Operator;
import operators.ProjectOperator;
import operators.SelectOperator;
import stormComponents.StormDataSource;
import stormComponents.synchronization.TopologyKiller;
import org.apache.log4j.Logger;
import queryPlans.QueryPlan;
import stormComponents.StormComponent;
import schema.ColumnNameType;
import utilities.MyUtilities;
import utilities.SystemParameters;

public class DataSourceComponent implements Component {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DataSourceComponent.class);
    
    private String _componentName;
    private String _inputPath;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private List<ColumnNameType> _tableSchema;
    private StormDataSource _dataSource;

    private ChainOperator _chain = new ChainOperator();

    private boolean _printOut;
    private boolean _printOutSet; // whether printOut condition is already set

    private Component _child;

    public DataSourceComponent(String componentName,
                        String inputPath,
                        List<ColumnNameType> tableSchema,
                        QueryPlan queryPlan) {
		_componentName=componentName;
		_inputPath=inputPath;
                _tableSchema = tableSchema;

                queryPlan.add(this);
    }

    @Override
    public DataSourceComponent setFullHashList(List<String> fullHashList){
        throw new RuntimeException("This method should not be invoked for DataSourceComponent!");
    }

    @Override
    public List<String> getFullHashList(){
        throw new RuntimeException("This method should not be invoked for DataSourceComponent!");
    }

    @Override
    public DataSourceComponent setHashIndexes(List<Integer> hashIndexes){
        _hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public DataSourceComponent setHashExpressions(List<ValueExpression> hashExpressions){
        _hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public DataSourceComponent addOperator(Operator operator){
	_chain.addOperator(operator);
        return this;
    }

    @Override
    public ChainOperator getChainOperator(){
        return _chain;
    }

    @Override
    public DataSourceComponent setPrintOut(boolean printOut){
        _printOutSet = true;
        _printOut = printOut;
        return this;
    }

    @Override
    public DataSourceComponent setBatchOutputMode(long millis){
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

        int parallelism = SystemParameters.getInt(conf, _componentName+"_PAR");
        if(parallelism > 1 && _chain.getDistinct() != null){
            throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiple spouts for one input file!");
        }

        MyUtilities.checkBatchOutput(_batchOutputMillis, _chain.getAggregation(), conf);

        _dataSource = new StormDataSource(
                _componentName,
                allCompNames,
               _inputPath,
               _hashIndexes,
               _hashExpressions,
               _chain,
               hierarchyPosition,
               _printOut,
               _batchOutputMillis,
               parallelism,
               builder,
               killer,
               conf);
    }

    @Override
    public Component[] getParents() {
        return null;
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
        return _tableSchema.size();
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources(){
        List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
        list.add(this);
        return list;
    }

    // from StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
        return _dataSource.getEmitterIDs();
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
        return _dataSource.getInfoID() + "\n";
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
        int hash = 3;
        hash = 59 * hash + (this._componentName != null ? this._componentName.hashCode() : 0);
        return hash;
    }

}