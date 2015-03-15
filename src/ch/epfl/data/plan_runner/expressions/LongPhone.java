package ch.epfl.data.plan_runner.expressions;

import java.util.List;

import ch.epfl.data.plan_runner.conversion.LongConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.visitors.ValueExpressionVisitor;

//translates phone in a form with dashes (e.g 88-9) into a Long
public class LongPhone implements ValueExpression<Long> {
	private static final long serialVersionUID = 1L;

	private final TypeConversion<Long> _wrapper = new LongConversion();

	private int _columnIndex;
	private int _firstDigits = -1;

	public LongPhone(int columnIndex) {
		_columnIndex = columnIndex;
	}

	public LongPhone(int columnIndex, int firstDigits) {
		this(columnIndex);
		_firstDigits = firstDigits;
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		throw new RuntimeException("Not implemented for a moment!");
		// vev.visit(this);
	}

	// unused
	@Override
	public void changeValues(int i, ValueExpression<Long> newExpr) {
		// nothing
	}

	@Override
	public Long eval(List<String> tuple) {
		String value = tuple.get(_columnIndex);
		value = value.replace("-", "");
		if (_firstDigits != -1) {
			value = value.substring(0, _firstDigits);
		}
		return _wrapper.fromString(value);
	}

	@Override
	public String evalString(List<String> tuple) {
		final long result = eval(tuple);
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
		sb.append("LongPhone ").append(_columnIndex);
		return sb.toString();
	}
}