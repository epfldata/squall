package predicates;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import visitors.PredicateVisitor;

public class AndPredicate implements Predicate {
    private static final long serialVersionUID = 1L;

    private List<Predicate> _predicateList = new ArrayList<Predicate>();

    public AndPredicate(Predicate pred1, Predicate pred2,
            Predicate... predicateArray){
        _predicateList.add(pred1);
        _predicateList.add(pred2);
        _predicateList.addAll(Arrays.asList(predicateArray));
    }

    @Override
    public boolean test(List<String> tupleValues) {
    	for (Predicate pred: _predicateList){
            if(pred.test(tupleValues)==false)
                return false;
            }
	return true;
    }

    @Override
    public List<Predicate> getInnerPredicates() {
        return _predicateList;
    }

    @Override
    public void accept(PredicateVisitor pv) {
        pv.visit(this);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for(int i=0; i< _predicateList.size(); i++){
            sb.append("(").append(_predicateList.get(i)).append(")");
            if(i!=_predicateList.size()-1){
                sb.append(" AND ");
            }
        }
        return sb.toString();
    }
}