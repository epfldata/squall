package ch.epfl.data.plan_runner.data_extractors.fluctuations;

public class GenerateOptimalMappings {

    private int _multFactor;
    private long _numberOfRelation1, _numberOfRelation2;

    private long _currentRelationCount1 = 0;
    private long _currentRelationCount2 = 0;
    int n, m;

    public GenerateOptimalMappings(long numberOfRelation1,
	    long numberOfRelation2, int multFactor, int initialN, int initialM) {
	_multFactor = multFactor;
	_numberOfRelation1 = numberOfRelation1;
	_numberOfRelation2 = numberOfRelation2;
	n = initialN;
	m = initialM;

	process();
    }

    private void process() {

	_currentRelationCount1 = 1;
	int currentRelationPointer = 2;
	long nextToStop = 0;
	int numOfOptimalMigrations = 0;

	while (_currentRelationCount1 < _numberOfRelation1
		|| _currentRelationCount2 < _numberOfRelation2) {

	    if (currentRelationPointer == 2) {

		if (_currentRelationCount1 < _numberOfRelation1)
		    nextToStop = (_currentRelationCount1 * _multFactor)
			    - _currentRelationCount2;
		else
		    nextToStop = _numberOfRelation2;
		for (int i = 0; i < nextToStop; i++) {
		    if (_currentRelationCount2 < _numberOfRelation2) {
			_currentRelationCount2++;
			// System.out.println(_currentRelationCount1+","+_currentRelationCount2);
			// check for change !!
			int[] newMapping = findOptimal(_currentRelationCount1,
				_currentRelationCount2, n, m);
			if (newMapping[0] > 0) {
			    // change triggered
			    numOfOptimalMigrations++;
			    n = newMapping[0];
			    m = newMapping[1];
			    System.out.println("Changing mapping at: "
				    + _currentRelationCount1 + ","
				    + _currentRelationCount2 + " to (" + n
				    + "," + m + ")");
			}
		    } else {
			currentRelationPointer = 1;
			break;
		    }
		}
		currentRelationPointer = 1;
	    } else if (currentRelationPointer == 1) {
		if (_currentRelationCount2 < _numberOfRelation2)
		    nextToStop = (_currentRelationCount2 * _multFactor)
			    - _currentRelationCount1;
		else
		    nextToStop = _numberOfRelation1;

		for (int i = 0; i < nextToStop; i++) {
		    if (_currentRelationCount1 < _numberOfRelation1) {
			_currentRelationCount1++;
			// System.out.println(_currentRelationCount1+","+_currentRelationCount2);
			// check for change !!
			int[] newMapping = findOptimal(_currentRelationCount1,
				_currentRelationCount2, n, m);
			if (newMapping[0] > 0) {
			    // change triggered
			    numOfOptimalMigrations++;
			    n = newMapping[0];
			    m = newMapping[1];
			    System.out.println("Changing mapping at: "
				    + _currentRelationCount1 + ","
				    + _currentRelationCount2 + " to (" + n
				    + "," + m + ")");
			}
		    } else {
			currentRelationPointer = 2;
			break;
		    }
		}
		currentRelationPointer = 2;
	    }
	}

	System.out.println("Finalized at: " + _currentRelationCount1 + ","
		+ _currentRelationCount2 + " to (" + n + "," + m + ")"
		+ " with number of optimal migrations "
		+ numOfOptimalMigrations);

    }

    // return optimal n, m
    private int[] findOptimal(long countR, long countS, int n, int m) {
	int[] res = new int[2];
	int[] prev = { n / 2, m * 2 }; // -1
	int[] next = { 2 * n, m / 2 }; // 1

	int minIndex = 0;

	double min = ((double) countR) / n + ((double) countS) / m;
	double value;
	if (n != 1) {
	    value = ((double) countR) / prev[0] + ((double) countS) / prev[1];
	    if (value < min) {
		value = min;
		minIndex = -1;
	    }
	}

	if (m != 1) {
	    value = ((double) countR) / next[0] + ((double) countS) / next[1];
	    if (value < min) {
		value = min;
		minIndex = 1;
	    }
	}

	if (minIndex == 0) {
	    res[0] = -1;
	    res[1] = -1;
	} else if (minIndex == -1) {
	    res[0] = n / 2;
	    res[1] = m * 2;
	} else if (minIndex == 1) {
	    res[0] = 2 * n;
	    res[1] = m / 2;
	}

	return res;
    }

    public static void main(String[] args) {
	GenerateOptimalMappings g = new GenerateOptimalMappings(10000000,
		10000000, 4, 4, 4);

    }

}
