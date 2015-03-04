package plan_runner.expressions;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import plan_runner.conversion.DateConversion;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.visitors.ValueExpressionVisitor;

public class DateDiff implements ValueExpression<Integer> {
	private static final long serialVersionUID = 1L;

	private final TypeConversion<Date> _dc = new DateConversion();
	private final TypeConversion<Integer> _ic = new IntegerConversion();

	private final ValueExpression<Date> _ve1, _ve2;

	public DateDiff(ValueExpression<Date> ve1, ValueExpression<Date> ve2) {
		_ve1 = ve1;
		_ve2 = ve2;
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		vev.visit(this);
	}

	@Override
	public void changeValues(int i, ValueExpression<Integer> newExpr) {
		// nothing
	}

	@Override
	public Integer eval(List<String> tuple) {		
		Date dateObj1 = _ve1.eval(tuple);
		Date dateObj2 = _ve2.eval(tuple);
		
		long diff = dateObj2.getTime() - dateObj1.getTime();
		int diffDays =  (int) (diff / (24* 1000 * 60 * 60));

		return diffDays;
	}

	@Override
	public String evalString(List<String> tuple) {
		return _ic.toString(eval(tuple));
	}

	@Override
	public List<ValueExpression> getInnerExpressions() {
		final List<ValueExpression> result = new ArrayList<ValueExpression>();
		result.add(_ve1);
		result.add(_ve2);
		return result;
	}

	@Override
	public TypeConversion getType() {
		return _ic;
	}

	@Override
	public void inverseNumber() {
		// nothing
	}

	@Override
	public boolean isNegative() {
		// nothing
		return false;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("DateDiff ");
		sb.append("First Exp: ").append(_ve1.toString()).append("\n");
		sb.append("Second Exp: ").append(_ve2.toString()).append("\n");		
		return sb.toString();
	}
}