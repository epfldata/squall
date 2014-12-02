package plan_runner.utilities.thetajoin.dynamic;

public class BufferedTuple {

	private final String _componentName;
	private final String _tupleString;
	private final String _tupleHash;

	public BufferedTuple(String componentName, String tupleString, String tupleHash) {
		_componentName = componentName;
		_tupleString = tupleString;
		_tupleHash = tupleHash;
	}

	public String get_componentName() {
		return _componentName;
	}

	public String get_tupleHash() {
		return _tupleHash;
	}

	public String get_tupleString() {
		return _tupleString;
	}

}
