package plan_runner.visitors;

import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.LikePredicate;
import plan_runner.predicates.OrPredicate;


public interface PredicateVisitor {

    public void visit(AndPredicate and);
    public void visit(BetweenPredicate between);
    public void visit(ComparisonPredicate comparison);
    public void visit(LikePredicate like);
    public void visit(OrPredicate or);
    
}
