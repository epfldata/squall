/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package predicates;

import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import visitors.PredicateVisitor;

/* This class is syntactic sugar for complex AndPredicate
 */
public class BetweenPredicate<T extends Comparable<T>> implements Predicate {
    private static Logger LOG = Logger.getLogger(BetweenPredicate.class);

    private Predicate _and;
    
    public BetweenPredicate(ValueExpression<T> ve, 
            boolean includeLower, ValueExpression<T> veLower, 
            boolean includeUpper, ValueExpression<T> veUpper){

        // set up boundaries correctly
        int opLower = ComparisonPredicate.GREATER_OP;
        if(includeLower){
            opLower = ComparisonPredicate.NONLESS_OP;
        }
        int opUpper = ComparisonPredicate.LESS_OP;
        if(includeUpper){
            opUpper = ComparisonPredicate.NONGREATER_OP;
        }

        // create syntactic sugar
        Predicate lower = new ComparisonPredicate(opLower, ve, veLower);
        Predicate upper = new ComparisonPredicate(opUpper, ve, veUpper);
        _and = new AndPredicate(lower, upper);        
    }

    @Override
    public List<Predicate> getInnerPredicates() {
        List<Predicate> result = new ArrayList<Predicate>();
        result.add(_and);
        return result;
    }


    @Override
    public boolean test(List<String> tupleValues) {
        return _and.test(tupleValues);
    }
    
    @Override
    public boolean test(List<String> firstTupleValues, List<String> secondTupleValues) {
        return _and.test(firstTupleValues, secondTupleValues);
    }

    @Override
    public void accept(PredicateVisitor pv) {
        pv.visit(this);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("BETWEEN implemented as AND: ").append(_and.toString());
        return sb.toString();
    }

}