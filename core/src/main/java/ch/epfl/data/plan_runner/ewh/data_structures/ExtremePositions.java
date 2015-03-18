package ch.epfl.data.plan_runner.ewh.data_structures;

public class ExtremePositions {
	private int _mostLeft, _mostRight;

	public ExtremePositions(int mostLeft, int mostRight) {
		_mostLeft = mostLeft;
		_mostRight = mostRight;
	}

	public int getMostLeft() {
		return _mostLeft;
	}

	public int getMostRight() {
		return _mostRight;
	}

	public void setMostLeft(int mostLeft) {
		_mostLeft = mostLeft;
	}

	public void setMostRight(int mostRight) {
		_mostRight = mostRight;
	}
}