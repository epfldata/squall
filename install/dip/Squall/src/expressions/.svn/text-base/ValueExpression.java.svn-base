/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package expressions;

import conversion.TypeConversion;
import java.io.Serializable;
import java.util.List;
import visitors.ValueExpressionVisitor;

public interface ValueExpression<T extends Comparable<T>> extends Serializable{
    public T eval(List<String> tuple);
    public String evalString(List<String> tuple);

    public TypeConversion getType();

    public void accept(ValueExpressionVisitor vev);
    //not ValueExpression<T> because inside might be other type(as in IntegerYearFromDate)
    public List<ValueExpression> getInnerExpressions();
}