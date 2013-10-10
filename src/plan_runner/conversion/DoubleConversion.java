package plan_runner.conversion;

public class DoubleConversion implements NumericConversion<Double> {
	private static final long serialVersionUID = 1L;

	@Override
	public Double fromDouble(double d) {
		return d;
	}

	@Override
	public Double fromString(String str) {
		return Double.valueOf(str);
	}

	@Override
	public double getDistance(Double bigger, Double smaller) {
		return bigger - smaller;
	}

	@Override
	public Double getInitialValue() {
		return new Double(0.0);
	}

	@Override
	public double toDouble(Object obj) {
		return (Double) obj;
	}

	// for printing(debugging) purposes
	@Override
	public String toString() {
		return "DOUBLE";
	}

	@Override
	public String toString(Double obj) {
		return obj.toString();
	}

}