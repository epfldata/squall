package plan_runner.expressions;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.visitors.ValueExpressionVisitor;

public class IntegerYearFromDate implements ValueExpression<Integer> {
	private static final long serialVersionUID = 1L;

	private final ValueExpression<Date> _veDate;
	private final TypeConversion<Integer> _wrapper = new IntegerConversion();

	public IntegerYearFromDate(ValueExpression<Date> veDate) {
		_veDate = veDate;
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		vev.visit(this);
	}

	/*
	 * @Override public Integer eval(List<String> firstTuple, List<String>
	 * secondTuple) { Date date = _veDate.eval(firstTuple, secondTuple);
	 * Calendar c = Calendar.getInstance(); c.setTime(date); int year =
	 * c.get(Calendar.YEAR);
	 * // Alternative approach: //SimpleDateFormat formatNowYear = new
	 * SimpleDateFormat("yyyy"); //String currentYear =
	 * formatNowYear.format(date); // = '2006'
	 * return year; }
	 */

	@Override
	public void changeValues(int i, ValueExpression<Integer> newExpr) {
		// nothing

	}

	@Override
	public Integer eval(List<String> tuple) {
		final Date date = _veDate.eval(tuple);

		final Calendar c = Calendar.getInstance();
		c.setTime(date);
		final int year = c.get(Calendar.YEAR);

		/*
		 * Alternative approach: SimpleDateFormat formatNowYear = new
		 * SimpleDateFormat("yyyy"); String currentYear =
		 * formatNowYear.format(date); // = '2006'
		 */

		return year;
	}

	@Override
	public String evalString(List<String> tuple) {
		final int result = eval(tuple);
		return _wrapper.toString(result);
	}

	@Override
	public List<ValueExpression> getInnerExpressions() {
		final List<ValueExpression> result = new ArrayList<ValueExpression>();
		result.add(_veDate);
		return result;
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
		sb.append("IntegerYearFromDate ").append(_veDate.toString());
		return sb.toString();
	}

}