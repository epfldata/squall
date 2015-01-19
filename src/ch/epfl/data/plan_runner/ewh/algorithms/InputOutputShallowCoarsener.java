package ch.epfl.data.plan_runner.ewh.algorithms;

import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;
import ch.epfl.data.plan_runner.utilities.MyUtilities;

public class InputOutputShallowCoarsener extends OutputShallowCoarsener {
    private static Logger LOG = Logger
	    .getLogger(InputOutputShallowCoarsener.class);

    public InputOutputShallowCoarsener(int numXBuckets, int numYBuckets,
	    WeightFunction wf, Map map, ADJUST_MODE amode, ITER_MODE imode) {
	super(numXBuckets, numYBuckets, wf, map, amode, imode);
    }

    public InputOutputShallowCoarsener(int numXBuckets, int numYBuckets,
	    WeightFunction wf, Map map) {
	super(numXBuckets, numYBuckets, wf, map);
    }

    // the following methods I might want to overload
    // weight = a * input + b * output
    @Override
    protected double computeMaxGridCellWeight() {
	// in the worst case, maxGridCellWeight is
	// * for pxp partitioning, m/p
	// * for pxq partitioning, m/(min(p,q))
	int minSideLength = findMin(_numXBuckets, _numYBuckets);
	int totalNumOutputs = _originalMatrix.getTotalNumOutputs();
	int frequency = totalNumOutputs / minSideLength;
	int halfPerimeter = _originalMatrix.getXSize() / _numXBuckets
		+ _originalMatrix.getYSize() / _numYBuckets;

	double result = MAX_WEIGHT_BADNESS * (2 + EPSILON)
		* _wf.getWeight(halfPerimeter, frequency);
	// LOG.info("Max grid cell weight is " + result);
	return result;
    }

    // weight = a * input + b * output
    @Override
    protected double getWeight(Region region) {
	return _wp.getWeight(region);
    }

    // returns 0 if the region contains no output
    @Override
    protected double getWeightEmpty0(Region region) {
	// too expensive and does not yield better partitioning than only
	// "return _wp.getWeight(region);" (tried on one example though):
	if (_wp.getFrequency(region) == 0
		&& !MyUtilities.isCandidateRegion(_originalMatrix, region,
			_originalMatrix.getComparisonPredicate(), _map)) {
	    // if (_wp.getFrequency(region) == 0){
	    // input can be arbitrarily large if it contains no output
	    // Would it be an improvement if we assign non-zero weights for
	    // empty regions which are candidates?
	    return 0;
	} else {
	    return _wp.getWeight(region);
	}
    }

    @Override
    public String toString() {
	return "InputOutputBinaryCoarsener [numXBuckets = " + _numXBuckets
		+ ", numYBuckets = " + _numYBuckets + "]";
    }

}