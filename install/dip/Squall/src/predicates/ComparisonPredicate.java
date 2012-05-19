package predicates;

import org.apache.log4j.Logger;

import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import visitors.PredicateVisitor;

public  class ComparisonPredicate<T extends Comparable<T>> implements Predicate {
    private static Logger LOG = Logger.getLogger(ComparisonPredicate.class);

    public static final int EQUAL_OP=0;
    public static final int NONEQUAL_OP=1;
    public static final int LESS_OP=2;
    public static final int NONLESS_OP=3;
    public static final int GREATER_OP=4;
    public static final int NONGREATER_OP=5;

    private ValueExpression<T> _ve1, _ve2;
    private int _operation;
    
    public ComparisonPredicate(int operation, ValueExpression<T> ve1, ValueExpression<T> ve2){
      _operation = operation;
      _ve1 = ve1;
      _ve2 = ve2;
    }
    
    public ComparisonPredicate(ValueExpression<T> ve1, ValueExpression<T> ve2){
      this(EQUAL_OP, ve1, ve2);
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
        Comparable val1 = (Comparable) _ve1.eval(tupleValues);
        Comparable val2 = (Comparable) _ve2.eval(tupleValues);
        int compared = val1.compareTo(val2);

        boolean result = false;
        switch(_operation){
            case EQUAL_OP:
                result = (compared == 0);
                break;
            case NONEQUAL_OP:
                result = (compared != 0);
                break;                
            case LESS_OP:
                result = (compared < 0);
                break;
            case NONLESS_OP:
                result = (compared >= 0);
                break;
            case GREATER_OP:
                result = (compared > 0);
                break;
            case NONGREATER_OP:
                result = (compared <= 0);
                break;
            default:
                throw new RuntimeException("Unsupported operation " + _operation);
        }
        return result;
    }
    
    @Override
    public boolean test(List<String> firstTupleValues, List<String> secondTupleValues){
        Comparable val1 = (Comparable) _ve1.eval(firstTupleValues, secondTupleValues);
        Comparable val2 = (Comparable) _ve2.eval(firstTupleValues, secondTupleValues);
        int compared = val1.compareTo(val2);

        boolean result = false;
        switch(_operation){
            case EQUAL_OP:
                result = (compared == 0);
                break;
            case NONEQUAL_OP:
                result = (compared != 0);
                break;                
            case LESS_OP:
                result = (compared < 0);
                break;
            case NONLESS_OP:
                result = (compared >= 0);
                break;
            case GREATER_OP:
                result = (compared > 0);
                break;
            case NONGREATER_OP:
                result = (compared <= 0);
                break;
            default:
                throw new RuntimeException("Unsupported operation " + _operation);
        }
        return result;
    }

    @Override
    public void accept(PredicateVisitor pv) {
        pv.visit(this);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(_ve1.toString());
        sb.append(getOperationStr());
        sb.append(_ve2.toString());
        return sb.toString();
    }

    private String getOperationStr(){
        String result = null;
        switch (_operation){
            case NONEQUAL_OP:
                result = " != ";
                break;
            case EQUAL_OP:
                result = " = ";
                break;
            case LESS_OP:
                result = " < ";
                break;
            case NONLESS_OP:
                result = " >= ";
                break;
            case GREATER_OP:
                result = " > ";
                break;
            case NONGREATER_OP:
                result = " <= ";
                break;
        }
        return result;
    }
}