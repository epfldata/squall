/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package expressions;

import conversion.NumericConversion;
import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import utilities.MyUtilities;
import visitors.ValueExpressionVisitor;

/*
 * This class implements Addition between any Number type (Integer, Double, Long, etc.).
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
public class Addition<T extends Number & Comparable<T>> implements ValueExpression<T> {

    private static final long serialVersionUID = 1L;

    private List<ValueExpression<T>> _veList = new ArrayList<ValueExpression<T>>();
    private NumericConversion<T> _wrapper;

    public Addition(NumericConversion<T> wrapper, ValueExpression<T> ve1, ValueExpression<T> ve2,
            ValueExpression<T>... veArray){
        _veList.add(ve1);
        _veList.add(ve2);
        _veList.addAll(Arrays.asList(veArray));
        _wrapper = wrapper;
    }

    @Override
    public T eval(List<String> tuple){
        double result = 0;
        for(ValueExpression<T> factor: _veList){
            T value = (T)factor.eval(tuple);
            result += _wrapper.toDouble(value);
        }
        return _wrapper.fromDouble(result);
    }
    
    @Override
    public T eval(List<String> firstTuple, List<String> secondTuple){
        double result = 0;
        for(ValueExpression<T> factor: _veList){
            T value = (T)factor.eval(firstTuple, secondTuple);
            result += _wrapper.toDouble(value);
        }
        return _wrapper.fromDouble(result);
    }

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
                sb.append(" + ");
            }
        }
        return sb.toString();
    }

}