package ch.epfl.data.squall.expressions;

import java.util.List;

import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.visitors.ValueExpressionVisitor;

//translates double 123.45 to 12345
public class DoubleToInt implements ValueExpression<Integer> {
	private static final long serialVersionUID = 1L;

	private final TypeConversion<Integer> _wrapper = new IntegerConversion();

	private int _columnIndex;

	public DoubleToInt(int columnIndex) {
		_columnIndex = columnIndex;
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		throw new RuntimeException("Not implemented for a moment!");
		// vev.visit(this);
	}

	// unused
	@Override
	public void changeValues(int i, ValueExpression<Integer> newExpr) {
		// nothing
	}

	@Override
	public Integer eval(List<String> tuple) {
		String value = tuple.get(_columnIndex);
		value = value.replace(".", "");
		return _wrapper.fromString(value);
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
	public void inverseNumber() {
		// nothing

	}

	@Override
	public boolean isNegative() {
		return false;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("DoubleToInt ").append(_columnIndex);
		return sb.toString();
	}
}