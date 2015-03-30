package ch.epfl.data.squall.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.visitors.ValueExpressionVisitor;

/*
 * This class implements Division between any Number type (Integer, Double, Long, etc.).
 * It convert all the value to double, and then return the final result by automatic casting
 *   (i.e. (int)1.0 )
 *
 * Double can store integers exactly in binary representation,
 *   so we won't lose the precision on our integer operations.
 *
 * Having different T types in the constructor arguments
 *   does not result in exception in the constructor,
 *   but rather in eval method.
 */
public class Division implements ValueExpression<Integer> {

	private static final long serialVersionUID = 1L;

	private final List<ValueExpression> _veList = new ArrayList<ValueExpression>();
	private final NumericConversion<Integer> _wrapper = new IntegerConversion();

	public Division(ValueExpression ve1, ValueExpression ve2,
			ValueExpression... veArray) {
		_veList.add(ve1);
		_veList.add(ve2);
		_veList.addAll(Arrays.asList(veArray));
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		vev.visit(this);
	}

	@Override
	public void changeValues(int i, ValueExpression<Integer> newExpr) {

	}

	@Override
	public Integer eval(List<String> tuple) {
		final ValueExpression firstVE = _veList.get(0);
		final Object firstObj = firstVE.eval(tuple);
		final NumericConversion firstType = (NumericConversion) firstVE
				.getType();
		double result = firstType.toDouble(firstObj);

		for (int i = 1; i < _veList.size(); i++) {
			final ValueExpression currentVE = _veList.get(i);
			final Object currentObj = currentVE.eval(tuple);
			final NumericConversion currentType = (NumericConversion) currentVE
					.getType();
			result /= currentType.toDouble(currentObj);
		}
		return _wrapper.fromDouble(result);
	}

	@Override
	public String evalString(List<String> tuple) {
		final Integer result = eval(tuple);
		return _wrapper.toString(result);
	}

	@Override
	public List<ValueExpression> getInnerExpressions() {
		return _veList;
	}

	@Override
	public TypeConversion getType() {
		return _wrapper;
	}

	@Override
	public void inverseNumber() {

	}

	@Override
	public boolean isNegative() {
		return false;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < _veList.size(); i++) {
			sb.append("(").append(_veList.get(i)).append(")");
			if (i != _veList.size() - 1)
				sb.append(" / ");
		}
		return sb.toString();
	}

}