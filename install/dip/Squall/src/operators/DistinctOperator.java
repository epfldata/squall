/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package operators;

import java.util.Map;
import java.util.List;
import utilities.MyUtilities;
import storage.SquallStorage;
import expressions.ValueExpression;

public class DistinctOperator implements Operator {

	private Map _conf;
	private int _numTuplesProcessed;
	private ProjectionOperator _projection;
	private static final long serialVersionUID = 1L;
	private SquallStorage _storage = new SquallStorage();
	/* Dummy value to associate with a tuple in the backing Storage (Since
	 * the backing storage provides a key-value interface) */
	private static final String dummyString = new String("dummy");

	public DistinctOperator(Map conf, ValueExpression ... veArray){
		_projection = new ProjectionOperator(veArray);
		_conf = conf;
	}

	public DistinctOperator(Map conf, List<ValueExpression> veList){
		_projection = new ProjectionOperator(veList);
		_conf = conf;
	}

	public DistinctOperator(Map conf, int[] projectionIndexes){
		_projection = new ProjectionOperator(projectionIndexes);
		_conf = conf;
	}

	public ProjectionOperator getProjection(){
		return _projection;
	}

	/* If tuple is present in the collection, return null,
	   otherwise, return projected tuple */
	@Override
	public List<String> process(List<String> tuple) {
		_numTuplesProcessed++;
		List<String> projectedTuple = _projection.process(tuple);
		String projectedTupleString = MyUtilities.tupleToString(projectedTuple, this._conf);
		if ( this._storage.contains(projectedTupleString) == true ){
			return null;
		} else {
			_storage.put(projectedTupleString, dummyString);
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
	public int getNumTuplesProcessed(){
		return _numTuplesProcessed;
	}

	@Override
	public List<String> getContent() {
		throw new RuntimeException("getContent for DistinctOperator should never be invoked!");
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("DistinctOperator with Projection: ");
		sb.append(_projection.toString());
		return sb.toString();
	}

}
