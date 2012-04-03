/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package operators;

import expressions.ValueExpression;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


public class DistinctOperator implements Operator{
    private static final long serialVersionUID = 1L;

    private ProjectionOperator _projection;

    private int _invocations;
    private HashSet<List<String>> _distinctTuples = new HashSet<List<String>>();

    public DistinctOperator(ValueExpression ... veArray){
        _projection = new ProjectionOperator(veArray);
    }
    
    public DistinctOperator(List<ValueExpression> veList){
        _projection = new ProjectionOperator(veList);
    }

    public DistinctOperator(int[] projectionIndexes){
        _projection = new ProjectionOperator(projectionIndexes);
    }

    public ProjectionOperator getProjection(){
        return _projection;
    }

    /* If tuple is present in the collection, return null,
       otherwise, return projected tuple */
    @Override
    public List<String> process(List<String> tuple) {
        _invocations++;
        List<String> projectedTuple = _projection.process(tuple);
        if(_distinctTuples.contains(projectedTuple)){
            return null;
        }else{
            _distinctTuples.add(projectedTuple);
            return projectedTuple;
        }
    }

    @Override
    public boolean isBlocking() {
        return false;
    }

    @Override
    public String printContent() {
        throw new RuntimeException("printContent for DistinctOperator should never be invoked!");
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("DistinctOperator with Projection: ");
        sb.append(_projection.toString());
        return sb.toString();
    }

}