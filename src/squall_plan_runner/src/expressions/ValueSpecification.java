/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package expressions;

import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.List;
import visitors.ValueExpressionVisitor;

/*
 * Having different T types in the constructor arguments
 *   does not result in exception in the constructor,
 *   but rather in evalString method.
 */
public class ValueSpecification<T extends Comparable<T>> implements ValueExpression<T> {
    private static final long serialVersionUID = 1L;

    private T _constant;
    private TypeConversion<T> _wrapper;

    public ValueSpecification(TypeConversion<T> wrapper, T constant){
        _constant = constant;
        _wrapper = wrapper;
    }

    @Override
    public T eval(List<String> tuple) {
        return _constant;
    }
    
    @Override
    public T eval(List<String> firstTuple, List<String> secondTuple) {
    	return _constant;
    }

    @Override
    public String evalString(List<String> tuple) {
        T value = eval(tuple);
        return _wrapper.toString(value);
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
        return new ArrayList<ValueExpression>();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("ValueSpecification - constant ").append(_constant.toString());
        return sb.toString();
    }
}