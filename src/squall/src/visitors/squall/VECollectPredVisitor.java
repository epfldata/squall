/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors.squall;

import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import predicates.AndPredicate;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;
import predicates.LikePredicate;
import predicates.OrPredicate;
import predicates.Predicate;
import visitors.PredicateVisitor;


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
