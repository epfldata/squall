package plan_runner.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.Addition;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.storage.AggregationStorage;
import plan_runner.storage.BasicStore;
import plan_runner.utilities.MyUtilities;
import plan_runner.visitors.OperatorVisitor;

public class AggregateSumOperator<T extends Number & Comparable<T>> implements AggregateOperator<T> {
        private static final long serialVersionUID = 1L;
        private static Logger LOG = Logger.getLogger(AggregateSumOperator.class);

        //the GroupBy type
        private static final int GB_UNSET = -1;
        private static final int GB_COLUMNS = 0;
        private static final int GB_PROJECTION =1;

        private DistinctOperator _distinct;
        private int _groupByType = GB_UNSET;
        private List<Integer> _groupByColumns = new ArrayList<Integer>();
        private ProjectOperator _groupByProjection;
        private int _numTuplesProcessed = 0;
        
        private NumericConversion _wrapper;
        private ValueExpression<T> _ve;
        private AggregationStorage<T> _storage;
        
        private Map _map;

        public AggregateSumOperator(ValueExpression<T> ve, Map map){
            _wrapper = (NumericConversion) ve.getType();
            _ve=ve;
            _map = map;
            _storage = new AggregationStorage<T>(this, _wrapper, _map, true);
        }

        //from AgregateOperator
        @Override
        public AggregateSumOperator<T> setGroupByColumns(List<Integer> groupByColumns) {
            if(!alreadySetOther(GB_COLUMNS)){
                _groupByType = GB_COLUMNS;
                _groupByColumns = groupByColumns;
                _storage.setSingleEntry(false);
                return this;
            }else{
                throw new RuntimeException("Aggragation already has groupBy set!");
            }
        }

        @Override
        public AggregateSumOperator setGroupByProjection(ProjectOperator groupByProjection) {
            if(!alreadySetOther(GB_PROJECTION)){
                _groupByType = GB_PROJECTION;
                _groupByProjection = groupByProjection;
                _storage.setSingleEntry(false);
                return this;
            }else{
                throw new RuntimeException("Aggragation already has groupBy set!");
            }
        }

        @Override
        public AggregateSumOperator setDistinct(DistinctOperator distinct){
            _distinct = distinct;
            return this;
        }

        @Override
        public List<Integer> getGroupByColumns() {
            return _groupByColumns;
        }

        @Override
        public ProjectOperator getGroupByProjection() {
            return _groupByProjection;
        }

        @Override
        public DistinctOperator getDistinct() {
            return _distinct;
        }

        @Override
        public List<ValueExpression> getExpressions() {
            List<ValueExpression> result = new ArrayList<ValueExpression>();
            result.add(_ve);
            return result;
        }
        
        @Override
        public boolean hasGroupBy(){
            return _groupByType != GB_UNSET;
        }        
        
        @Override
        public TypeConversion getType(){
            return _wrapper;
        }        

        //from Operator
        @Override
        public List<String> process(List<String> tuple){
            _numTuplesProcessed++;
            if(_distinct != null){
                tuple = _distinct.process(tuple);
                if(tuple == null){
                    return null;
                }
            }
            String tupleHash;
            if(_groupByType == GB_PROJECTION){
                tupleHash = MyUtilities.createHashString(tuple, _groupByColumns, _groupByProjection.getExpressions(), _map);
            }else{
                tupleHash = MyUtilities.createHashString(tuple, _groupByColumns, _map);
            }
            T value = _storage.update(tuple, tupleHash);
            String strValue = _wrapper.toString(value);

            // propagate further the affected tupleHash-tupleValue pair
            List<String> affectedTuple = new ArrayList<String>();
            affectedTuple.add(tupleHash);
            affectedTuple.add(strValue);
            
            return affectedTuple;
        }

        //actual operator implementation
        @Override
        public T runAggregateFunction(T value, List<String> tuple) {
            ValueExpression<T> base = new ValueSpecification<T>(_wrapper, value);
            Addition<T> result = new Addition<T>(base, _ve);
            return result.eval(tuple);
        }

        @Override
        public T runAggregateFunction(T value1, T value2) {
            ValueExpression<T> ve1 = new ValueSpecification<T>(_wrapper, value1);
            ValueExpression<T> ve2 = new ValueSpecification<T>(_wrapper, value2);
            Addition<T> result = new Addition<T>(ve1, ve2);
            return result.eval(null);
        }

        @Override
        public boolean isBlocking() {
            return true;
        }

        @Override
        public BasicStore getStorage(){
            return _storage;
        }

        @Override
        public void clearStorage(){
            _storage.reset();
        }

        @Override
        public int getNumTuplesProcessed(){
            return _numTuplesProcessed;
        }

        @Override
	public String printContent(){
            return _storage.getContent();
        }

        //for this method it is essential that HASH_DELIMITER, which is used in tupleToString method,
        //  is the same as DIP_GLOBAL_ADD_DELIMITER
        @Override
        public List<String> getContent(){
	    String str = _storage.getContent(); 
            return str == null ? null : Arrays.asList(str.split("\\r?\\n"));
        }

        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append("AggregateSumOperator with VE: ");
            sb.append(_ve.toString());
            if(_groupByColumns.isEmpty() && _groupByProjection == null){
                sb.append("\n  No groupBy!");
            }else if (!_groupByColumns.isEmpty()){
                sb.append("\n  GroupByColumns are ").append(getGroupByStr()).append(".");
            }else if (_groupByProjection != null){
                sb.append("\n  GroupByProjection is ").append(_groupByProjection.toString()).append(".");
            }
            if(_distinct!=null){
                sb.append("\n  It also has distinct ").append(_distinct.toString());
            }
            return sb.toString();
        }

        private String getGroupByStr(){
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for(int i=0; i<_groupByColumns.size(); i++){
                sb.append(_groupByColumns.get(i));
                if(i==_groupByColumns.size()-1){
                    sb.append(")");
                }else{
                    sb.append(", ");
                }
            }
            return sb.toString();
        }

    private boolean alreadySetOther(int GB_COLUMNS) {
        return (_groupByType != GB_COLUMNS && _groupByType != GB_UNSET);
    }

    @Override
    public void accept(OperatorVisitor ov){
        ov.visit(this);
    }
                
}
