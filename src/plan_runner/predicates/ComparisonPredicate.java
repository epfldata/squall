package plan_runner.predicates;

import java.util.ArrayList;
import java.util.List;
import plan_runner.expressions.ValueExpression;
import plan_runner.visitors.PredicateVisitor;

public  class ComparisonPredicate<T extends Comparable<T>> implements Predicate {

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
	// YANNIS: HACK FOR TPCH6
	if (val2 instanceof Long) {
		val2 = (Double)(((Long)val2).doubleValue());
	}
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
        Comparable val1 = (Comparable) _ve1.eval(firstTupleValues);
        Comparable val2 = (Comparable) _ve2.eval(secondTupleValues);
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
    
    public T getType(){
    	return (T) _ve1.getType().getInitialValue();
    }
    
    public int getOperation(){
    	return _operation;
    }
    
    public int getOperator(boolean inverse)
    {
    	int result = 0;
    	if (inverse)
    	{
            switch (_operation){
            case NONEQUAL_OP:
                result = NONEQUAL_OP;
                break;
            case EQUAL_OP:
                result = EQUAL_OP;
                break;
            case LESS_OP:
                result = GREATER_OP;
                break;
            case NONLESS_OP:
                result = NONGREATER_OP;
                break;
            case GREATER_OP:
                result = LESS_OP;
                break;
            case NONGREATER_OP:
                result = NONLESS_OP;
                break;
            }
        }
    	else
    	{
    		result = _operation;
    	}
    	return result;
    }

}
