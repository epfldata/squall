package ch.epfl.data.plan_runner.predicates;

import java.io.Serializable;
import java.util.List;

import ch.epfl.data.plan_runner.visitors.PredicateVisitor;

public interface Predicate extends Serializable {
	public void accept(PredicateVisitor pv);

	public List<Predicate> getInnerPredicates();

	public boolean test(List<String> tupleValues);

	public boolean test(List<String> firstTupleValues, List<String> secondTupleValues);

	@Override
	public String toString();
}
