package plan_runner.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import plan_runner.expressions.ValueExpression;
import plan_runner.storage.BasicStore;
import plan_runner.storage.ValueStore;
import plan_runner.utilities.MyUtilities;
import plan_runner.visitors.OperatorVisitor;

public class DistinctOperator implements Operator {

	private final Map _conf;
	private int _numTuplesProcessed;
	private final ProjectOperator _projection;
	private static final long serialVersionUID = 1L;
	private final BasicStore<ArrayList<String>> _storage;
	/*
	 * Dummy value to associate with a tuple in the backing Storage (Since the
	 * backing storage provides a key-value interface)
	 */
	private static final String dummyString = new String("dummy");

	public DistinctOperator(Map conf, int[] projectionIndexes) {
		_storage = new ValueStore<String>(conf);
		_projection = new ProjectOperator(projectionIndexes);
		_conf = conf;
	}

	public DistinctOperator(Map conf, List<ValueExpression> veList) {
		_storage = new ValueStore<String>(conf);
		_projection = new ProjectOperator(veList);
		_conf = conf;
	}

	public DistinctOperator(Map conf, ValueExpression... veArray) {
		_storage = new ValueStore<String>(conf);
		_projection = new ProjectOperator(veArray);
		_conf = conf;
	}

	@Override
	public void accept(OperatorVisitor ov) {
		ov.visit(this);
	}

	@Override
	public List<String> getContent() {
		throw new RuntimeException("getContent for DistinctOperator should never be invoked!");
	}

	@Override
	public int getNumTuplesProcessed() {
		return _numTuplesProcessed;
	}

	public ProjectOperator getProjection() {
		return _projection;
	}

	@Override
	public boolean isBlocking() {
		return false;
	}

	@Override
	public String printContent() {
		throw new RuntimeException("printContent for DistinctOperator should never be invoked!");
	}

	/*
	 * If tuple is present in the collection, return null, otherwise, return
	 * projected tuple
	 */
	@Override
	public List<String> process(List<String> tuple) {
		_numTuplesProcessed++;
		final List<String> projectedTuple = _projection.process(tuple);
		final String projectedTupleString = MyUtilities.tupleToString(projectedTuple, _conf);
		if (_storage.contains(projectedTupleString) == true)
			return null;
		else {
			_storage.insert(projectedTupleString, dummyString);
			return tuple;
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("DistinctOperator with Projection: ");
		sb.append(_projection.toString());
		return sb.toString();
	}

}
