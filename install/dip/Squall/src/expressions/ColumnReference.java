/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package expressions;

import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.List;
import visitors.ValueExpressionVisitor;

public class ColumnReference<T extends Comparable<T>> implements ValueExpression<T> {
    private static final long serialVersionUID = 1L;

    private int _column;
    private TypeConversion<T> _wrapper;

    public ColumnReference(TypeConversion<T> wrapper, int column){
        _column = column;
        _wrapper = wrapper;
    }

    @Override
    public T eval(List<String> tuple){
        String value = tuple.get(_column);
        return _wrapper.fromString(value);
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


    public int getColumnIndex(){
        return _column;
    }

    public void setColumnIndex(int column){
        _column = column;
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
        sb.append("ColumnReference ").append(_column);
        return sb.toString();
    }


}