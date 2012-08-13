package plan_runner.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.utilities.MyUtilities;
import plan_runner.visitors.ValueExpressionVisitor;

/*
 * This class implements Multiplication between any Number type (Integer, Double, Long, etc.).
 * It convert all the value to double, and then return the final result by automatic casting
 *   (i.e. (int)1.0 )
 *
 * Double can store integers exatly in binary representation,
 *   so we won't lose the precision on our integer operations.
 *
 * Having different T types in the constructor arguments
 *   does not result in exception in the constructor,
 *   but rather in eval method.
 */
public class Multiplication<T extends Number & Comparable<T>> implements ValueExpression<T> {

    private static final long serialVersionUID = 1L;

    private List<ValueExpression<T>> _veList = new ArrayList<ValueExpression<T>>();
    private NumericConversion<T> _wrapper;

    public Multiplication(ValueExpression<T> ve1, ValueExpression<T> ve2,
            ValueExpression<T>... veArray){
        _veList.add(ve1);
        _veList.add(ve2);
        _veList.addAll(Arrays.asList(veArray));
        _wrapper = (NumericConversion<T>) MyUtilities.getDominantNumericType(MyUtilities.listTypeErasure(_veList));
    }

    @Override
    public T eval(List<String> tuple){
        double result = 1;
        for(ValueExpression<T> factor: _veList){
            T value = (T)factor.eval(tuple);
            result *= _wrapper.toDouble(value);
        }
        return _wrapper.fromDouble(result);
    }
    
    /*@Override
    public T eval(List<String> firstTuple, List<String> secondTuple){
        double result = 1;
        
    	ValueExpression<T> factor = _veList.get(0);
        T value = (T)factor.eval(firstTuple);
        result *= _wrapper.toDouble(value);
        
        factor = _veList.get(1);
        value = (T)factor.eval(secondTuple);
        result *= _wrapper.toDouble(value);

        return _wrapper.fromDouble(result);
    }*/

     @Override
    public String evalString(List<String> tuple) {
        T result = eval(tuple);
        return _wrapper.toString(result);
    }

    @Override
    public TypeConversion getType(){
        return _wrapper;
    }

    @Override
    public void accept(ValueExpressionVisitor vev) {
        vev.visit(this);
    }

    @Override
    public List<ValueExpression> getInnerExpressions() {
        return MyUtilities.listTypeErasure(_veList);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<_veList.size(); i++){
            sb.append("(").append(_veList.get(i)).append(")");
            if(i!=_veList.size()-1){
                sb.append(" * ");
            }
        }
        return sb.toString();
    }

    @Override
	public void changeValues(int i, ValueExpression<T> newExpr) {
    	_veList.remove(i);
    	_veList.add(i, newExpr);		
	}

	@Override
	public void inverseNumber() {
		//nothing
		
	}

	@Override
	public boolean isNegative() {
		return false;
	}

}