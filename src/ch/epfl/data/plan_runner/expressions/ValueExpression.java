package ch.epfl.data.plan_runner.expressions;

import java.io.Serializable;
import java.util.List;

import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.visitors.ValueExpressionVisitor;

public interface ValueExpression<T extends Comparable<T>> extends Serializable {
    public void accept(ValueExpressionVisitor vev);

    // matt...
    public void changeValues(int i, ValueExpression<T> newExpr);

    public T eval(List<String> tuple);

    public String evalString(List<String> tuple);

    // not ValueExpression<T> because inside might be other type(as in
    // IntegerYearFromDate)
    public List<ValueExpression> getInnerExpressions();

    public TypeConversion getType();

    public void inverseNumber();

    public boolean isNegative();
}