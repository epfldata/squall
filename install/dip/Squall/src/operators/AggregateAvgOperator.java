/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package operators;

import conversion.DoubleConversion;
import conversion.NumericConversion;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;
import utilities.MyUtilities;

public class AggregateAvgOperator implements AggregateOperator {
        private static final long serialVersionUID = 1L;
        private static Logger LOG = Logger.getLogger(AggregateAvgOperator.class);

        //the GroupBy type
        private static final int GB_UNSET = -1;
        private static final int GB_COLUMNS = 0;
        private static final int GB_PROJECTION = 1;

        private DistinctOperator _distinct;
        private int _groupByType = GB_UNSET;
        private List<Integer> _groupByColumns = new ArrayList<Integer>();
        private ProjectionOperator _groupByProjection;
        private int _invocations = 0;
        
        private NumericConversion<Double> _wrapper = new DoubleConversion();
        private ValueExpression<Double> _ve;
        private HashMap<String, SumCount> _aggregateAvg = new HashMap<String, SumCount>();

        private Map _map;

        public AggregateAvgOperator(ValueExpression<Double> ve, Map map){
            _ve=ve;
            _map=map;
        }

        //from AgregateOperator
        @Override
        public AggregateAvgOperator setGroupByColumns(List<Integer> groupByColumns) {
             if(!alreadySetOther(GB_COLUMNS)){
                _groupByType = GB_COLUMNS;
                _groupByColumns = groupByColumns;
                return this;
            }else{
                throw new RuntimeException("Aggragation already has groupBy set!");
            }
        }

        @Override
        public AggregateAvgOperator setGroupByProjection(ProjectionOperator groupByProjection){
             if(!alreadySetOther(GB_PROJECTION)){
                _groupByType = GB_PROJECTION;
                _groupByProjection = groupByProjection;
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
        public ProjectionOperator getGroupByProjection(){
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
            _invocations++;
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
            Double value = updateContent(tuple, tupleHash);
            String strValue = _wrapper.toString(value);

            // propagate further the affected tupleHash-tupleValue pair
            ArrayList<String> affectedTuple = new ArrayList<String>();
            affectedTuple.add(tupleHash);
            affectedTuple.add(strValue);

            return affectedTuple;
        }

        private Double updateContent(List<String> tuple, String tupleHash){
            SumCount sumCount = _aggregateAvg.get(tupleHash);
            if(sumCount == null){
                sumCount = new SumCount();
            }
            sumCount = runAggregateFunction(sumCount, tuple);
            _aggregateAvg.put(tupleHash, sumCount);
            return sumCount.getAvg();
        }

        //actual operator implementation
        private SumCount runAggregateFunction(SumCount value, List<String> tuple) {
            Double sumDelta = _ve.eval(tuple);
            Integer countDelta = 1;
            
            Double sumNew = sumDelta + value.getSum();
            Integer countNew = countDelta + value.getCount();
            
            return new SumCount(sumNew, countNew);
        }

        @Override
        public boolean isBlocking() {
            return true;
        }

        @Override
	public String printContent(){
            StringBuilder sb = new StringBuilder();
            sb.append("Iteration ").append(_invocations).append(":\n");
            Iterator<Entry<String, SumCount>> it = _aggregateAvg.entrySet().iterator();
	    while (it.hasNext()) {
	       Map.Entry pairs = (Map.Entry) it.next();
	       SumCount value = (SumCount) pairs.getValue();
	       sb.append(pairs.getKey()).append(" = ").append(value.getAvg()).append("\n");
	    }
	    sb.append("----------------------------------\n");
            return sb.toString();
        }

        @Override
        public List<String> getContent() {
            throw new UnsupportedOperationException("getContent for AggregateAvgOperator is not supported yet.");
        }

        private class SumCount{
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