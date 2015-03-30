package ch.epfl.data.squall.visitors;

import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.BetweenPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.LikePredicate;
import ch.epfl.data.squall.predicates.OrPredicate;
import ch.epfl.data.squall.predicates.booleanPrimitive;

public interface PredicateVisitor {

	public void visit(AndPredicate and);

	public void visit(BetweenPredicate between);

	public void visit(booleanPrimitive bool);

	public void visit(ComparisonPredicate comparison);

	public void visit(LikePredicate like);

	public void visit(OrPredicate or);

}
