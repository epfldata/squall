package ch.epfl.data.plan_runner.expressions;

import java.util.List;

import ch.epfl.data.plan_runner.conversion.LongConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.visitors.ValueExpressionVisitor;

// concatenates an integer key (e.g. Orders.Custkey) with a double / 10.000 (e.g. Orders.TotalPrice)
//     this fits in integer even for 320G TPCH database
public class ConcatIntDouble implements ValueExpression<Long> {
	private static final long serialVersionUID = 1L;

	private final TypeConversion<Long> _wrapper = new LongConversion();

	private int _intIndex;
	private int _dblIndex;
	private int _divider;
	private DoubleToInt _dti;

	private static final int DECIMALS = 2; // decimal places after .
	private static final int DOUBLE_TO_INT = (int) Math.pow(10, DECIMALS);

	public ConcatIntDouble(int intIndex, int dblIndex, int divider) {
		_intIndex = intIndex;
		_dblIndex = dblIndex;
		_divider = divider;
		_dti = new DoubleToInt(_dblIndex);
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
		int intValue1 = Integer.parseInt(tuple.get(_intIndex));
		int intValue2 = _dti.eval(tuple);
		long result = intValue1 + (intValue2 / (_divider * DOUBLE_TO_INT))
				* 100000000L;
		// throw new RuntimeException("Ulazi su " + tuple.get(_intIndex) +
		// " and  " + tuple.get(_dblIndex) + ", a izlaz je " + result);
		return result;
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
		sb.append("ConcatIntString ").append(_intIndex).append(", ")
				.append(_dblIndex);
		return sb.toString();
	}
}