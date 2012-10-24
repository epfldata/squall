package plan_runner.expressions;

import java.io.Serializable;
import java.util.List;
import plan_runner.conversion.TypeConversion;
import plan_runner.visitors.ValueExpressionVisitor;

public interface ValueExpression<T extends Comparable<T>> extends Serializable{
    public T eval(List<String> tuple);
  //  public T eval(List<String> firstTuple, List<String> secondTuple);
    
    public String evalString(List<String> tuple);

    public TypeConversion getType();

    public void accept(ValueExpressionVisitor vev);
    //not ValueExpression<T> because inside might be other type(as in IntegerYearFromDate)
    public List<ValueExpression> getInnerExpressions();
    
    //matt...
    public void changeValues(int i, ValueExpression<T> newExpr);
    public void inverseNumber();
    public boolean isNegative();
}