package predicates;

import java.io.Serializable;
import java.util.List;
import visitors.PredicateVisitor;

public interface Predicate extends Serializable  {
    public boolean test(List<String> tupleValues);
    public boolean test(List<String> firstTupleValues, List<String> secondTupleValues);
    
    public List<Predicate> getInnerPredicates();

    public void accept(PredicateVisitor pv);
}
