/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package operators;

import conversion.DoubleConversion;
import conversion.NumericConversion;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import operators.AggregateAvgOperator.SumCount;
import storage.BasicStore;
import storage.HashMapAggStorage;
import org.apache.log4j.Logger;
import utilities.MyUtilities;

public class AggregateAvgOperator implements AggregateOperator<SumCount> {
        private static final long serialVersionUID = 1L;
        private static Logger LOG = Logger.getLogger(AggregateAvgOperator.class);

        //the GroupBy type
        private static final int GB_UNSET = -1;
        private static final int GB_COLUMNS = 0;
        private static final int GB_PROJECTION = 1;

        private DistinctOperator _distinct;
        private int _groupByType = GB_UNSET;
        private List<Integer> _groupByColumns = new ArrayList<Integer>();
        private ProjectOperator _groupByProjection;
        private int _numTuplesProcessed = 0;
        
        private NumericConversion<Double> _wrapper = new DoubleConversion();
        private ValueExpression<Double> _ve;
        private BasicStore<SumCount> _storage;

        private Map _map;

        public AggregateAvgOperator(ValueExpression<Double> ve, Map map){
            _ve=ve;
            _map=map;

            _storage = new HashMapAggStorage<SumCount>(this, _wrapper, _map, true);
        }

        //from AgregateOperator
        @Override
        public AggregateAvgOperator setGroupByColumns(List<Integer> groupByColumns) {
             if(!alreadySetOther(GB_COLUMNS)){
                _groupByType = GB_COLUMNS;
                _groupByColumns = groupByColumns;
                _storage = new HashMapAggStorage<SumCount>(this, _wrapper, _map, false);
                return this;
            }else{
                throw new RuntimeException("Aggragation already has groupBy set!");
            }
        }

        @Override
        public AggregateAvgOperator setGroupByProjection(ProjectOperator groupByProjection){
             if(!alreadySetOther(GB_PROJECTION)){
                _groupByType = GB_PROJECTION;
                _groupByProjection = groupByProjection;
                _storage = new HashMapAggStorage<SumCount>(this, _wrapper, _map, false);
                return this;
            }else{
                throw new RuntimeException("Aggragation already has groupBy set!");
            }
        }

        @Override
        public AggregateAvgOperator setDistinct(DistinctOperator distinct){
            _distinct = distinct;
            return this;
        }

        @Override
        public List<Integer> getGroupByColumns() {
            return _groupByColumns;
        }

        @Override
        public ProjectOperator getGroupByProjection(){
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
            SumCount sumCount = _storage.update(tuple, tupleHash);
            Double value = sumCount.getAvg();
            String strValue = _wrapper.toString(value);

            // propagate further the affected tupleHash-tupleValue pair
            List<String> affectedTuple = new ArrayList<String>();
            affectedTuple.add(tupleHash);
            affectedTuple.add(strValue);

            return affectedTuple;
        }

        //actual operator implementation
        @Override
        public SumCount runAggregateFunction(SumCount value, List<String> tuple) {
            Double sumDelta = _ve.eval(tuple);
            Integer countDelta = 1;
            
            Double sumNew = sumDelta + value.getSum();
            Integer countNew = countDelta + value.getCount();
            
            return new SumCount(sumNew, countNew);
        }

        @Override
        public SumCount runAggregateFunction(SumCount value1, SumCount value2) {
            Double sumNew = value1.getSum() + value2.getSum();
            Integer countNew = value1.getCount() + value2.getCount();
            return new SumCount(sumNew, countNew);
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

        @Override
        public List<String> getContent() {
            throw new UnsupportedOperationException("getContent for AggregateAvgOperator is not supported yet.");
        }

        public class SumCount{
            private Double _sum;
            private Integer _count;

            public SumCount(Double sum, Integer count){
                _sum = sum;
                _count = count;
            }

            public SumCount(){
                this(0.0,0);
            }
 
            public Double getSum() {
                return _sum;
            }

            public void setSum(Double sum) {
                _sum = sum;
            }
     
            public Integer getCount() {
                return _count;
            }

            public void setCount(Integer count) {
                _count = count;
            }

            public double getAvg(){
                return _sum/_count;
            }

            @Override
            public String toString(){
                return String.valueOf(getAvg());
            }

            @Override
            public boolean equals(Object obj){
                if(this == obj){
                    return true;
                }
                if (!(obj instanceof SumCount)){
                    return false;
                }
                SumCount otherSumCount = (SumCount) obj;
                return getAvg() == otherSumCount.getAvg();
            }

            @Override
            public int hashCode() {
                int hash = 7;
                hash = 89 * hash + (_sum != null ? _sum.hashCode() : 0);
                hash = 89 * hash + (_count != null ? _count.hashCode() : 0);
                return hash;
            }
        }

        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append("AggregateAvgOperator with VE: ");
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
}
