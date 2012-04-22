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
import stormComponents.StormDataSource;
import stormComponents.synchronization.Flusher;
import stormComponents.synchronization.TopologyKiller;
import stormComponents.synchronization.TrafficLight;
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

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private List<ColumnNameType> _tableSchema;
    private StormDataSource[] _dataSources;

    private SelectionOperator _selection;
    private ProjectionOperator _projection;
    private DistinctOperator _distinct;
    private AggregateOperator _aggregation;

    private boolean _printOut;

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
    public DataSourceComponent setSelection(SelectionOperator selection){
	_selection=selection;
        return this;
    }

    @Override
    public DataSourceComponent setDistinct(DistinctOperator distinct) {
        _distinct = distinct;
        return this;
    }

    @Override
    public DataSourceComponent setProjection(ProjectionOperator projection){
        _projection=projection;
        return this;
    }

    @Override
    public DataSourceComponent setAggregation(AggregateOperator aggregation) {
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
    public DataSourceComponent setPrintOut(){
        _printOut = true;
        return this;
    }

    @Override
    public void makeBolts(TopologyBuilder builder,
                       TopologyKiller killer,
                       Flusher flusher,
                       TrafficLight trafficLight,
                       Config conf,
                       int partitioningType,
                       int hierarchyPosition){

        //by default print out for the last component
        //for other conditions, can be set via setPrintOut
        if(hierarchyPosition==StormComponent.FINAL_COMPONENT){
                setPrintOut();
        }

        int parallelism = SystemParameters.getInt(conf, _componentName+"_PAR");
        if(parallelism > 1 && _distinct != null){
            throw new RuntimeException(_componentName + ": Distinct operator cannot be specified for multiple spouts for one input file!");
        }

        _dataSources = new StormDataSource[parallelism];
        for (int i=0; i<parallelism; i++){
            _dataSources[i] = new StormDataSource(_componentName,
               _inputPath,
               _hashIndexes,
               _hashExpressions,
               _selection,
               _distinct,
               _projection,
               _aggregation,
               hierarchyPosition,
               _printOut,
               i,
               parallelism,
               builder,
               killer,
               flusher);
        }
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

    //TODO: not correct if we have projection, distinct or aggregation
    @Override
    public int getOutputSize(){
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
    public int[] getEmitterIDs() {
        int[] result = new int[]{};

        for(StormDataSource ds: _dataSources){
            result = MyUtilities.mergeArrays(result, ds.getEmitterIDs());
        }
        return result;
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
        StringBuilder sb = new StringBuilder();
        for(StormDataSource ds: _dataSources){
            sb.append(ds.getInfoID()).append("\n");
        }
        return sb.toString();
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