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
	public Double getOffset(Object base, double delta) {
		return (Double)base + delta;
	}

	@Override
	public Double getInitialValue() {
		return 0.0;
	}
	
	@Override
	public Double minIncrement(Object obj){
		return (Double)obj + getMinPositiveValue();
	}

	@Override
	public Double minDecrement(Object obj){
		return (Double)obj - getMinPositiveValue();
	}	
	
	@Override
	public Double getMinValue() {
		return -1 * Double.MAX_VALUE;
	}

	@Override
	public Double getMinPositiveValue() {
		return Double.MIN_VALUE;
	}	
	
	@Override
	public Double getMaxValue() {
		return Double.MAX_VALUE;
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