package plan_runner.operators;

import java.util.List;

import plan_runner.predicates.Predicate;
import plan_runner.visitors.OperatorVisitor;

public class SelectOperator implements Operator {
	private static final long serialVersionUID = 1L;

	private final Predicate _predicate;

	private int _numTuplesProcessed = 0;

	public SelectOperator(Predicate predicate) {
		_predicate = predicate;
	}

	@Override
	public void accept(OperatorVisitor ov) {
		ov.visit(this);
	}

	@Override
	public List<String> getContent() {
		throw new RuntimeException("getContent for SelectionOperator should never be invoked!");
	}

	@Override
	public int getNumTuplesProcessed() {
		return _numTuplesProcessed;
	}

	public Predicate getPredicate() {
		return _predicate;
	}

	@Override
	public boolean isBlocking() {
		return false;
	}

	@Override
	public String printContent() {
		throw new RuntimeException("printContent for SelectionOperator should never be invoked!");
	}

	@Override
	public List<String> process(List<String> tuple) {
		_numTuplesProcessed++;
		if (_predicate.test(tuple))
			return tuple;
		else
			return null;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("SelectOperator with Predicate: ");
		sb.append(_predicate.toString());
		return sb.toString();
	}
}
