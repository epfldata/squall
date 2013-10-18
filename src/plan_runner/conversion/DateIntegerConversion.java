package plan_runner.conversion;

public class DateIntegerConversion implements NumericConversion<Integer> {
	private static final long serialVersionUID = 1L;
	
	private static final DateConversion _dc = new DateConversion();

	@Override
	public Integer fromDouble(double d) {
		return (int) d;
	}

	@Override
	public Integer fromString(String str) {
		final String[] splits = str.split("-");
		final int year = Integer.parseInt(splits[0]) * 10000;
		final int month = Integer.parseInt(splits[1]) * 100;
		final int day = Integer.parseInt(splits[2]);
		return year + month + day;
	}

	@Override
	public double getDistance(Integer bigger, Integer smaller) {
		return _dc.getDistance(_dc.fromLong(bigger.longValue()), _dc.fromLong(smaller.longValue()));
	}

	@Override
	public Integer getInitialValue() {
		return new Integer(0);
	}

	@Override
	public double toDouble(Object obj) {
		return (Integer) obj;
	}

	@Override
	public String toString(Integer obj) {
		return obj.toString();
	}

}
