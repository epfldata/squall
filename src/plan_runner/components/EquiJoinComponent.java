package plan_runner.components;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.Operator;
import plan_runner.operators.ProjectOperator;
import plan_runner.query_plans.QueryPlan;
import plan_runner.storage.AggregationStorage;
import plan_runner.storage.BasicStore;
import plan_runner.storage.KeyValueStore;
import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormDstJoin;
import plan_runner.storm_components.StormJoin;
import plan_runner.storm_components.StormSrcJoin;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;

public class EquiJoinComponent implements Component {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(EquiJoinComponent.class);

    private Component _firstParent;
    private Component _secondParent;
    private Component _child;

    private String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private StormJoin _joiner;

    private ChainOperator _chain = new ChainOperator();

    // The storage is actually KeyValue<String, String>
    //    or AggregationStorage<Numeric> for pre-aggregation
    // Access method returns a list of Strings (a list of Numerics for pre-aggregation)
    private BasicStore<ArrayList<String>> _firstStorage, _secondStorage;
    //preAggregation
    private ProjectOperator _firstPreAggProj, _secondPreAggProj;

    private boolean _printOut;
    private boolean _printOutSet; //whether printOut was already set

    private List<String> _fullHashList;

    public EquiJoinComponent(Component firstParent,
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
    public EquiJoinComponent setFullHashList(List<String> fullHashList){
        _fullHashList = fullHashList;
        return this;
    }

    @Override
    public List<String> getFullHashList(){
        return _fullHashList;
    }

    @Override
    public EquiJoinComponent setHashIndexes(List<Integer> hashIndexes){
        _hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public EquiJoinComponent setHashExpressions(List<ValueExpression> hashExpressions){
        _hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public EquiJoinComponent addOperator(Operator operator){
	_chain.addOperator(operator);
        return this;
    }

    @Override
    public ChainOperator getChainOperator(){
        return _chain;
    }


    //next four methods are for Preaggregation
    public EquiJoinComponent setFirstPreAggStorage(AggregationStorage firstPreAggStorage){
        _firstStorage = firstPreAggStorage;
        return this;
    }

    public EquiJoinComponent setSecondPreAggStorage(AggregationStorage secondPreAggStorage){
        _secondStorage = secondPreAggStorage;
        return this;
    }
    
    //Out of the first storage (join of S tuple with R relation)
    public EquiJoinComponent setFirstPreAggProj(ProjectOperator firstPreAggProj){
        _firstPreAggProj = firstPreAggProj;
        return this;
    }

    //Out of the second storage (join of R tuple with S relation)
    public EquiJoinComponent setSecondPreAggProj(ProjectOperator secondPreAggProj){
        _secondPreAggProj = secondPreAggProj;
        return this;
    }

    @Override
    public EquiJoinComponent setPrintOut(boolean printOut){
        _printOutSet = true;
        _printOut = printOut;
        return this;
    }

    @Override
    public EquiJoinComponent setBatchOutputMillis(long millis){
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

        // If not set in Preaggregation, we set normal storages
        if(_firstStorage == null){
            _firstStorage = new KeyValueStore<String, String>(conf);
        }
        if(_secondStorage == null){
            _secondStorage = new KeyValueStore<String, String>(conf);
        }
        
        if(partitioningType == StormJoin.DST_ORDERING){
                _joiner = new StormDstJoin(_firstParent,
                                    _secondParent,
                                    this,
                                    allCompNames,
                                    _firstStorage,
                                    _secondStorage,
                                    _firstPreAggProj,
                                    _secondPreAggProj,
                                    hierarchyPosition,
                                    builder,
                                    killer,
                                    conf);
   
        }else if(partitioningType == StormJoin.SRC_ORDERING){
            if(_chain.getDistinct()!=null){
                throw new RuntimeException("Cannot instantiate Distinct operator from StormSourceJoin! There are two Bolts processing operators!");
            }

            //since we don't know how data is scattered across StormSrcStorage,
            //  we cannot do customStreamGrouping from the previous level
            _joiner = new StormSrcJoin(_firstParent,
                                    _secondParent,
                                    this,
                                    allCompNames,
                                    _firstStorage,
                                    _secondStorage,
                                    _firstPreAggProj,
                                    _secondPreAggProj,
                                    hierarchyPosition,
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
    public boolean getPrintOut() {
        return _printOut;
    }

    @Override
    public long getBatchOutputMillis() {
        return _batchOutputMillis;
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
