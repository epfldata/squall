package plan_runner.conversion;

import java.util.Date;

public class DateIntegerConversion implements NumericConversion<Integer>{

	@Override
	public Integer fromString(String str) {
		String[] splits=str.split("-");
		int year=Integer.parseInt(splits[0])*10000;
		int month=Integer.parseInt(splits[1])*100;
		int day=Integer.parseInt(splits[2]);
		return year+month+day;
	}

	@Override
	public String toString(Integer obj) {
		return obj.toString();
	}

	@Override
	public Integer getInitialValue() {
		return new Integer(0);
	}

	@Override
	public double getDistance(Integer bigger, Integer smaller) {
		return bigger-smaller;
	}

	@Override
	public Integer fromDouble(double d) {
		return (int)d;
	}

	@Override
	public double toDouble(Object obj) {
		return (Integer)obj;
	}

}
