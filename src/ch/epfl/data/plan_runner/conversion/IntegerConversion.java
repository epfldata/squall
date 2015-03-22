package ch.epfl.data.plan_runner.conversion;

public class IntegerConversion implements NumericConversion<Integer> {
	private static final long serialVersionUID = 1L;

	@Override
	public Integer fromDouble(double d) {
		return (int) d;
	}

	@Override
	public Integer fromString(String str) {
		return Integer.valueOf(str);
	}

	@Override
	public double getDistance(Integer bigger, Integer smaller) {
		return bigger.doubleValue() - smaller.doubleValue();
	}

	@Override
	public Integer getInitialValue() {
		return 0;
	}

	@Override
	public Integer getMaxValue() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Integer getMinPositiveValue() {
		return 1;
	}

	@Override
	public Integer getMinValue() {
		return Integer.MIN_VALUE;
	}

	@Override
	public Integer getOffset(Object base, double delta) {
		return (Integer) base + (int) delta;
	}

	@Override
	public Integer minDecrement(Object obj) {
		return (Integer) obj - getMinPositiveValue();
	}

	@Override
	public Integer minIncrement(Object obj) {
		return (Integer) obj + getMinPositiveValue();
	}

	@Override
	public double toDouble(Object obj) {
		final int value = (Integer) obj;
		return value;
	}

	// for printing(debugging) purposes
	@Override
	public String toString() {
		return "INTEGER";
	}

	@Override
	public String toString(Integer obj) {
		return obj.toString();
	}
}
