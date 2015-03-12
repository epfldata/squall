package ch.epfl.data.plan_runner.visitors;

import ch.epfl.data.plan_runner.predicates.AndPredicate;
import ch.epfl.data.plan_runner.predicates.BetweenPredicate;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.LikePredicate;
import ch.epfl.data.plan_runner.predicates.OrPredicate;
import ch.epfl.data.plan_runner.predicates.booleanPrimitive;

public interface PredicateVisitor {

	public void visit(AndPredicate and);

	public void visit(BetweenPredicate between);

	public void visit(ComparisonPredicate comparison);

	public void visit(LikePredicate like);

	public void visit(OrPredicate or);
	
	public void visit(booleanPrimitive bool);

}
