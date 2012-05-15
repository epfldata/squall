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
import stormComponents.StormDstJoin;
import stormComponents.StormJoin;
import stormComponents.StormSrcJoin;
import stormComponents.synchronization.TopologyKiller;
import org.apache.log4j.Logger;
import queryPlans.QueryPlan;
import stormComponents.StormComponent;
import utilities.MyUtilities;
import storage.SquallStorage;

public class JoinComponent implements Component {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(JoinComponent.class);

    private Component _firstParent;
    private Component _secondParent;
    private Component _child;

    private String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private StormJoin _joiner;

    private SelectionOperator _selection;
    private DistinctOperator _distinct;
    private ProjectionOperator _projection;
    private AggregateOperator _aggregation;

    //preAggregation
    private SquallStorage _firstPreAggStorage, _secondPreAggStorage;
    private ProjectionOperator _firstPreAggProj, _secondPreAggProj;

    private boolean _printOut;
    private boolean _printOutSet; //whether printOut was already set

    private List<String> _fullHashList;

    public JoinComponent(Component firstParent,
                    Component secondParent,
                    QueryPlan queryPlan){
      _firstParent = firstParent;
      _firstParent.setChild(this);
      _secondParent = secondParent;
      _secondParent.setChild(this);

      _componentName = firstParent.getName() + "_" + secondParent.getName();

      queryPlan.add(this);
    }

    //list of distinct keys, used for direct stream grouping and load-balancing ()
    @Override
    public JoinComponent setFullHashList(List<String> fullHashList){
        _fullHashList = fullHashList;
        return this;
    }

    @Override
    public List<String> getFullHashList(){
        return _fullHashList;
    }

    @Override
    public JoinComponent setHashIndexes(List<Integer> hashIndexes){
        _hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public JoinComponent setHashExpressions(List<ValueExpression> hashExpressions){
        _hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public JoinComponent setSelection(SelectionOperator selection){
        _selection = selection;
        return this;
    }

    @Override
    public JoinComponent setDistinct(DistinctOperator distinct){
        _distinct = distinct;
        return this;
    }

    @Override
    public JoinComponent setProjection(ProjectionOperator projection){
        _projection = projection;
        return this;
    }

    @Override
    public JoinComponent setAggregation(AggregateOperator aggregation){
        _aggregation = aggregation;
        return this;
    }

    //next four methods are for Preaggregation
    public JoinComponent setFirstPreAggStorage(SquallStorage firstPreAggStorage){
        _firstPreAggStorage = firstPreAggStorage;
        return this;
    }

    public JoinComponent setSecondPreAggStorage(SquallStorage secondPreAggStorage){
        _secondPreAggStorage = secondPreAggStorage;
        return this;
    }

    //Out of the first storage (join of S tuple with R relation)
    public JoinComponent setFirstPreAggProj(ProjectionOperator firstPreAggProj){
        _firstPreAggProj = firstPreAggProj;
        return this;
    }

    //Out of the second storage (join of R tuple with S relation)
    public JoinComponent setSecondPreAggProj(ProjectionOperator secondPreAggProj){
        _secondPreAggProj = secondPreAggProj;
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
    public JoinComponent setPrintOut(boolean printOut){
        _printOutSet = true;
        _printOut = printOut;
        return this;
    }

    @Override
    public JoinComponent setBatchOutputMode(long millis){
        _batchOutputMillis = millis;
        return this;
    }

    @Override
    public void makeBolts(TopologyBuilder builder,
            TopologyKiller killer,
            Config conf,
            int partitioningType,
            int hierarchyPosition){

        //by default print out for the last component
        //for other conditions, can be set via setPrintOut
        if(hierarchyPosition==StormComponent.FINAL_COMPONENT && !_printOutSet){
           setPrintOut(true);
        }

        MyUtilities.checkBatchOutput(_batchOutputMillis, _aggregation, conf);

        if(partitioningType == StormJoin.DST_ORDERING){
                //In Preaggregation one or two storages can be set; otherwise no storage is set
                if(_firstPreAggStorage == null){
                    _firstPreAggStorage = new SquallStorage();
                }
                if(_secondPreAggStorage == null){
                    _secondPreAggStorage = new SquallStorage();
                }

                _joiner = new StormDstJoin(_firstParent,
                                    _secondParent,
                                    _componentName,
                                    _selection,
                                    _distinct,
                                    _projection,
                                    _aggregation,
                                    _firstPreAggStorage,
                                    _secondPreAggStorage,
                                    _firstPreAggProj,
                                    _secondPreAggProj,
                                    _hashIndexes,
                                    _hashExpressions,
                                    hierarchyPosition,
                                    _printOut,
                                    _batchOutputMillis,
                                    _fullHashList,
                                    builder,
                                    killer,
                                    conf);
   
        }else if(partitioningType == StormJoin.SRC_ORDERING){
            if(_distinct!=null){
                throw new RuntimeException("Cannot instantiate Distinct operator from StormSourceJoin! There are two Bolts processing operators!");
            }
            //In Preaggregation one or two storages can be set; otherwise no storage is set
            if(_firstPreAggStorage == null){
                _firstPreAggStorage = new SquallStorage();
            }
            if(_secondPreAggStorage == null){
                _secondPreAggStorage = new SquallStorage();
            }

            //since we don't know how data is scattered across StormSrcStorage,
            //  we cannot do customStreamGrouping from the previous level
            _joiner = new StormSrcJoin(_firstParent,
                                    _secondParent,
                                    _componentName,
                                    _selection,
                                    _projection,
                                    _aggregation,
                                    _firstPreAggStorage,
                                    _secondPreAggStorage,
                                    _firstPreAggProj,
                                    _secondPreAggProj,
                                    _hashIndexes,
                                    _hashExpressions,
                                    hierarchyPosition,
                                    _printOut,
                                    _batchOutputMillis,
                                    builder,
                                    killer,
                                    conf);

        }else{
            throw new RuntimeException("Unsupported ordering " + partitioningType);
        }
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
    public int getOutputSize(){
        int joinColumnsLength = _firstParent.getHashIndexes().size();
        return _firstParent.getOutputSize() + _secondParent.getOutputSize() - joinColumnsLength;
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
    public int[] getEmitterIDs() {
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
