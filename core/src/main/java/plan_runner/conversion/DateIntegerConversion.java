package plan_runner.conversion;

import java.util.Calendar;
import java.util.Date;

import plan_runner.expressions.DateSum;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;

public class DateIntegerConversion implements NumericConversion<Integer> {
	private static final long serialVersionUID = 1L;
	
	private final DateConversion _dc = new DateConversion();

	@Override
	public Integer fromDouble(double d) {
		return (int) d;
	}

	@Override
	public Integer fromString(String str) {
		final String[] splits = str.split("-");
		final int year = Integer.parseInt(new String(splits[0])) * 10000;
		final int month = Integer.parseInt(new String(splits[1])) * 100;
		final int day = Integer.parseInt(new String(splits[2]));
		return year + month + day;
	}

	@Override
	public double getDistance(Integer bigger, Integer smaller) {
		return _dc.getDistance(_dc.fromInteger(bigger), _dc.fromInteger(smaller));
	}
	
	@Override
	public Integer getOffset(Object base, double delta) {
		return _dc.addDays((Integer)base, (int)delta);
	}

	@Override
	public Integer getInitialValue() {
		return 0;
	}
	
	@Override
	public Integer minIncrement(Object obj){
		return _dc.addDays((Integer)obj, 1);
	}

	@Override
	public Integer minDecrement(Object obj){
		return _dc.addDays((Integer)obj, -1);
	}	
	
	@Override
	public Integer getMinValue() {
		//return Integer.MIN_VALUE;
		return 18000101;
	}
	
	@Override
	public Integer getMinPositiveValue() {
		return 1;
	}	

	@Override
	public Integer getMaxValue() {
		//return Integer.MAX_VALUE;
		return 20200101;
	}

	@Override
	public double toDouble(Object obj) {
		return (Integer) obj;
	}

	@Override
	public String toString(Integer obj) {
		return obj.toString();
	}

	public String toStringWithDashes(Integer obj) {
		String strDate = obj.toString();
		return strDate.substring(0, 4) + "-" + strDate.substring(4, 6) + "-" + strDate.substring(6, 8);
	}
	
	public static void main(String[] args){
		DateIntegerConversion dcConv = new DateIntegerConversion();
		System.out.println(dcConv.getDistance(dcConv.getMaxValue(), dcConv.getMinValue()));
		System.out.println(dcConv.getDistance(dcConv.getMinValue(), dcConv.getMaxValue()));
		System.out.println(dcConv.getDistance(dcConv.getMaxValue(), dcConv.getMaxValue()));
		System.out.println(dcConv.getDistance(dcConv.getMinValue(), dcConv.getMinValue()));
		System.out.println(dcConv.minIncrement(dcConv.getMinValue()));
		System.out.println(dcConv.minDecrement(dcConv.getMaxValue()));
	}

}
