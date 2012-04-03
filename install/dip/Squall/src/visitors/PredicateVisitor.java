/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors;

import predicates.AndPredicate;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;
import predicates.OrPredicate;


public interface PredicateVisitor {

    public void visit(AndPredicate and);
    public void visit(BetweenPredicate between);
    public void visit(ComparisonPredicate comparison);
    public void visit(OrPredicate or);
    
}
