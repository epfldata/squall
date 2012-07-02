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

    private int _columnIndex;
    private String _columnStr;
    private TypeConversion<T> _wrapper;

    public ColumnReference(TypeConversion<T> wrapper, int columnIndex){
        _columnIndex = columnIndex;
        _wrapper = wrapper;
    }

    /*
     * columnStr is optional, used only in toString method
     */
    public ColumnReference(TypeConversion<T> wrapper, int columnIndex, String columnStr){
        this(wrapper, columnIndex);
        _columnStr = columnStr;
    }    
    
    @Override
    public T eval(List<String> tuple){
        String value = tuple.get(_columnIndex);
        return _wrapper.fromString(value);
    }
    
   /* @Override
    public T eval(List<String> firstTuple, List<String> secondTuple){
    	return null;
    }*/

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
        return _columnIndex;
    }

    public void setColumnIndex(int column){
        _columnIndex = column;
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
        if(_columnStr != null){
            sb.append("\"").append(_columnStr).append("\" - ");
        }
        sb.append("Col ").append(_columnIndex);
        return sb.toString();
    }

    @Override
	public void changeValues(int i, ValueExpression<T> newExpr) {
		//nothing
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