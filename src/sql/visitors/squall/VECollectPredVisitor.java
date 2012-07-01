package sql.visitors.squall;

import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.OrPredicate;
import plan_runner.predicates.LikePredicate;
import plan_runner.predicates.Predicate;
import plan_runner.expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import plan_runner.visitors.PredicateVisitor;


public class VECollectPredVisitor implements PredicateVisitor{
    private List<ValueExpression> _veList = new ArrayList<ValueExpression>();

    public List<ValueExpression> getExpressions(){
        return _veList;
    }

    @Override
    public void visit(AndPredicate and) {
        visit(and.getInnerPredicates());
    }

    @Override
    public void visit(BetweenPredicate between) {
        visit(between.getInnerPredicates());
    }

    @Override
    public void visit(OrPredicate or) {
        visit(or.getInnerPredicates());
    }

    private void visit(List<Predicate> predList){
        for(Predicate pred: predList){
            pred.accept(this);
        }
    }

    @Override
    public void visit(ComparisonPredicate comparison) {
        _veList.addAll(comparison.getExpressions());
    }

    @Override
    public void visit(LikePredicate like) {
        _veList.addAll(like.getExpressions());
    }

}
