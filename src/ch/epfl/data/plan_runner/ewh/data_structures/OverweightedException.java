package ch.epfl.data.plan_runner.ewh.data_structures;

public class OverweightedException extends Exception {
    private static final long serialVersionUID = 1L;
    private double _maxWeight; // what was asked for; not the region weight
    private int _x1, _y1, _x2, _y2; // region of the matrix with too high weight

    public OverweightedException(double maxWeight, int x1, int y1, int x2,
	    int y2) {
	_maxWeight = maxWeight;
	_x1 = x1;
	_y1 = y1;
	_x2 = x2;
	_y2 = y2;
    }

    @Override
    public String toString() {
	return "Impossible to achieve maxWeight less than " + _maxWeight
		+ ": [" + _x1 + ", " + _y1 + ", " + _x2 + ", " + _y2 + "]";
    }
}