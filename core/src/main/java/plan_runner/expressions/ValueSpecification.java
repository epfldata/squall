package plan_runner.expressions;

import java.util.ArrayList;
import java.util.List;

import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.visitors.ValueExpressionVisitor;

/*
 * Having different T types in the constructor arguments
 *   does not result in exception in the constructor,
 *   but rather in evalString method.
 */
public class ValueSpecification<T extends Comparable<T>> implements ValueExpression<T> {
	private static final long serialVersionUID = 1L;

	private T _constant;
	private final TypeConversion<T> _wrapper;

	public ValueSpecification(TypeConversion<T> wrapper, T constant) {
		_constant = constant;
		_wrapper = wrapper;
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		vev.visit(this);
	}

	@Override
	public void changeValues(int i, ValueExpression<T> newExpr) {

	}

	@Override
	public T eval(List<String> tuple) {
		return _constant;
	}

	@Override
	public String evalString(List<String> tuple) {
		final T value = eval(tuple);
		return _wrapper.toString(value);
	}

	@Override
	public List<ValueExpression> getInnerExpressions() {
		return new ArrayList<ValueExpression>();
	}

	@Override
	public TypeConversion getType() {
		return _wrapper;
	}

	@Override
	public void inverseNumber() {
		if (_wrapper instanceof NumericConversion) {
			final NumericConversion makis = (NumericConversion) _wrapper;
			// double temp = makis.toDouble((Number) _constant);
			final double val = ((Number) _constant).doubleValue();
			final double temp = makis.toDouble(new Double(val));
			_constant = (T) makis.fromDouble(1.0 / temp);
		}
	}

	@Override
	public boolean isNegative() {
		if (_wrapper instanceof NumericConversion) {
			final NumericConversion makis = (NumericConversion) _wrapper;
			final double temp = makis.toDouble(_constant);
			return (temp < 0);
		}
		return false;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("Constant ").append(_constant.toString());
		return sb.toString();
	}
}