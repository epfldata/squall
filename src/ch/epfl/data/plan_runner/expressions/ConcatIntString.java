package ch.epfl.data.plan_runner.expressions;

import java.util.List;

import ch.epfl.data.plan_runner.conversion.IntegerConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.visitors.ValueExpressionVisitor;

// concatenates an integer key (e.g. Orders.Custkey) with a string starting with a digit (e.g. Orders.Order-Priority = "5-Low")
//     this fits in integer even for 320G TPCH database
public class ConcatIntString implements ValueExpression<Integer> {
    private static final long serialVersionUID = 1L;

    private final TypeConversion<Integer> _wrapper = new IntegerConversion();

    private int _intIndex;
    private int _strIndex;

    public ConcatIntString(int intIndex, int strIndex) {
	_intIndex = intIndex;
	_strIndex = strIndex;
    }

    @Override
    public Integer eval(List<String> tuple) {
	int intValue1 = Integer.parseInt(tuple.get(_intIndex));
	int intValue2 = Integer.parseInt(tuple.get(_strIndex).substring(0, 1));
	return intValue1 + intValue2 * 100000000;
    }

    @Override
    public String evalString(List<String> tuple) {
	final int result = eval(tuple);
	return _wrapper.toString(result);
    }

    @Override
    public List<ValueExpression> getInnerExpressions() {
	return null;
    }

    @Override
    public TypeConversion getType() {
	return _wrapper;
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("ConcatIntString ").append(_intIndex).append(", ")
		.append(_strIndex);
	return sb.toString();
    }

    // unused
    @Override
    public void changeValues(int i, ValueExpression<Integer> newExpr) {
	// nothing
    }

    @Override
    public void inverseNumber() {
	// nothing

    }

    @Override
    public boolean isNegative() {
	return false;
    }

    @Override
    public void accept(ValueExpressionVisitor vev) {
	throw new RuntimeException("Not implemented for a moment!");
	// vev.visit(this);
    }
}