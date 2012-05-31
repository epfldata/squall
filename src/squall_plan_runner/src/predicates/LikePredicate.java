package predicates;

import conversion.StringConversion;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.ArrayList;
import java.util.List;
import visitors.PredicateVisitor;

/*
 * ve1 LIKE ve2 (bigger smaller)
 * WORKS ONLY for pattern '%value%'
 */
public  class LikePredicate implements Predicate {
    private ValueExpression<String> _ve1, _ve2;

    public LikePredicate(ValueExpression<String> ve1, ValueExpression<String> ve2){
      _ve1 = ve1;
      _ve2 = ve2;
      // WORKS ONLY for pattern '%value%'
      if(_ve2 instanceof ValueSpecification){
          String value = _ve2.eval(null);
          value = value.replace("%", "");
          _ve2 = new ValueSpecification<String>(new StringConversion(), value);
      }
    }

    public List<ValueExpression> getExpressions(){
        List<ValueExpression> result = new ArrayList<ValueExpression>();
        result.add(_ve1);
        result.add(_ve2);
        return result;
    }

    @Override
    public List<Predicate> getInnerPredicates() {
        return new ArrayList<Predicate>();
    }

    @Override
    public boolean test(List<String> tupleValues){
        String val1 = _ve1.eval(tupleValues);
        String val2 = _ve2.eval(tupleValues);
        return val1.contains(val2);
    }

    @Override
    public boolean test(List<String> firstTupleValues, List<String> secondTupleValues){
        String val1 = _ve1.eval(firstTupleValues);
        String val2 = _ve2.eval(firstTupleValues);
        return val1.contains(val2);
    }

    @Override
    public void accept(PredicateVisitor pv) {
        pv.visit(this);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(_ve1.toString());
        sb.append(" LIKE ");
        sb.append(_ve2.toString());
        return sb.toString();
    }

}