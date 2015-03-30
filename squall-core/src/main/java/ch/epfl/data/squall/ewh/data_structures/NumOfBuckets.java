package ch.epfl.data.squall.ewh.data_structures;

public class NumOfBuckets {
	private int _xNumOfBuckets, _yNumOfBuckets;

	public NumOfBuckets(int xNumOfBuckets, int yNumOfBuckets) {
		_xNumOfBuckets = xNumOfBuckets;
		_yNumOfBuckets = yNumOfBuckets;
	}

	public int getXNumOfBuckets() {
		return _xNumOfBuckets;
	}

	public int getYNumOfBuckets() {
		return _yNumOfBuckets;
	}
}