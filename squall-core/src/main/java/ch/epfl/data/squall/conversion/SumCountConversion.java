package ch.epfl.data.squall.conversion;

public class SumCountConversion implements TypeConversion<SumCount> {
	private static final long serialVersionUID = 1L;

	@Override
	public SumCount fromString(String sc) {
		final String parts[] = sc.split("\\:");
		final Double sum = Double.valueOf(new String(parts[0]));
		final Long count = Long.valueOf(new String(parts[1]));
		return new SumCount(sum, count);
	}

	@Override
	public double getDistance(SumCount bigger, SumCount smaller) {
		return bigger.getAvg() - smaller.getAvg();
	}

	@Override
	public SumCount getInitialValue() {
		return new SumCount(0.0, 0L);
	}

	// for printing(debugging) purposes
	@Override
	public String toString() {
		return "SUM_COUNT";
	}

	@Override
	public String toString(SumCount sc) {
		return sc.getSum() + ":" + sc.getCount();
	}

}