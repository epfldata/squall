package ch.epfl.data.plan_runner.conversion;

public class StringConversion implements TypeConversion<String> {
	private static final long serialVersionUID = 1L;

	@Override
	public String fromString(String str) {
		return str;
	}

	@Override
	public double getDistance(String bigger, String smaller) {
		throw new RuntimeException("Not applicable!");
	}

	@Override
	public String getInitialValue() {
		return "";
	}

	// for printing(debugging) purposes
	@Override
	public String toString() {
		return "STRING";
	}

	@Override
	public String toString(String obj) {
		return obj;
	}
}