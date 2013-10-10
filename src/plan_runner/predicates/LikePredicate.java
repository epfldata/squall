package plan_runner.predicates;

import java.util.ArrayList;
import java.util.List;

import plan_runner.conversion.StringConversion;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.visitors.PredicateVisitor;

/*
 * ve1 LIKE ve2 (bigger smaller)
 * WORKS ONLY for pattern '%value%'
 */
public class LikePredicate implements Predicate {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final ValueExpression<String> _ve1;
	private ValueExpression<String> _ve2;

	public LikePredicate(ValueExpression<String> ve1, ValueExpression<String> ve2) {
		_ve1 = ve1;
		_ve2 = ve2;
		// WORKS ONLY for pattern '%value%'
		if (_ve2 instanceof ValueSpecification) {
			String value = _ve2.eval(null);
			value = value.replace("%", "");
			_ve2 = new ValueSpecification<String>(new StringConversion(), value);
		}
	}

	@Override
	public void accept(PredicateVisitor pv) {
		pv.visit(this);
	}

	public List<ValueExpression> getExpressions() {
		final List<ValueExpression> result = new ArrayList<ValueExpression>();
		result.add(_ve1);
		result.add(_ve2);
		return result;
	}

	@Override
	public List<Predicate> getInnerPredicates() {
		return new ArrayList<Predicate>();
	}

	@Override
	public boolean test(List<String> tupleValues) {
		final String val1 = _ve1.eval(tupleValues);
		final String val2 = _ve2.eval(tupleValues);
		return val1.contains(val2);
	}

	@Override
	public boolean test(List<String> firstTupleValues, List<String> secondTupleValues) {
		final String val1 = _ve1.eval(firstTupleValues);
		final String val2 = _ve2.eval(firstTupleValues);
		return val1.contains(val2);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(_ve1.toString());
		sb.append(" LIKE ");
		sb.append(_ve2.toString());
		return sb.toString();
	}

}