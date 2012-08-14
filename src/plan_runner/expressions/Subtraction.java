package plan_runner.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.utilities.MyUtilities;
import plan_runner.visitors.ValueExpressionVisitor;

/*
 * This class implements Subtraction between any Number type (Integer, Double, Long, etc.).
 * It convert all the value to double, and then return the final result by automatic casting
 *   (i.e. (int) 1.0 )
 *
 * Double can store integers exatly in binary representation,
 *   so we won't lose the precision on our integer operations.
 *
 * Having different T types in the constructor arguments
 *   does not result in exception in the constructor,
 *   but rather in eval method.
 *
 * The formula applied on value expressions is: VE1 - VE2 - VE3 - ...
 */
public class Subtraction<T extends Number & Comparable<T>> implements ValueExpression<T> {
    private static final long serialVersionUID = 1L;

    private List<ValueExpression> _veList = new ArrayList<ValueExpression>();
    private NumericConversion<T> _wrapper;

    public Subtraction(ValueExpression ve1, ValueExpression ve2,
            ValueExpression... veArray){
        _veList.add(ve1);
        _veList.add(ve2);
        _veList.addAll(Arrays.asList(veArray));
        _wrapper = (NumericConversion<T>) MyUtilities.getDominantNumericType(_veList);
    }

    @Override
    public T eval(List<String> tuple){
        ValueExpression firstVE = _veList.get(0);
        Object firstObj = firstVE.eval(tuple);
        NumericConversion firstType = (NumericConversion) firstVE.getType();
        double result = firstType.toDouble(firstObj);

        for(int i = 1; i < _veList.size(); i++){
            ValueExpression currentVE = _veList.get(i);
            Object currentObj = currentVE.eval(tuple);
            NumericConversion currentType = (NumericConversion) currentVE.getType();
            result -= currentType.toDouble(currentObj);
        }
        return _wrapper.fromDouble(result);
        
    }
    
    
  /*  @Override
    public T eval(List<String> firstTuple, List<String> secondTuple){
        T first = _veList.get(0).eval(firstTuple);
        double result = _wrapper.toDouble(first);

        ValueExpression<T> factor = _veList.get(1);
        T value = (T)factor.eval(secondTuple);
        result -= _wrapper.toDouble(value);
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
        return _veList;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<_veList.size(); i++){
            sb.append("(").append(_veList.get(i)).append(")");
            if(i!=_veList.size()-1){
                sb.append(" - ");
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
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isNegative() {
		// TODO Auto-generated method stub
		return false;
	}


}