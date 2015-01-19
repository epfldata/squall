package ch.epfl.data.plan_runner.ewh.algorithms;

import java.util.Map;

import ch.epfl.data.plan_runner.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;

public class OkcanExactInputAlgorithm extends OkcanAlgorithm {
    // that's how the optimality would be checked
    private WeightFunction _wf;

    public OkcanExactInputAlgorithm(int j, WeightFunction wf, int numXBuckets,
	    int numYBuckets, Map map) {
	super(j, numXBuckets, numYBuckets, map, new OkcanExactCoarsener());
	_wf = wf;
    }

    // coarsened region
    @Override
    public double getWeight(Region coarsenedRegion) {
	// Currently, the actual input is at most maxInput * bucketSize of
	// bigger relation
	// TODO we should scale sides according to bucketXSize : bucketYSize
	// but they don't do this in the paper
	// This would require to change the bounds for binarySearch (increase
	// the upperBound)
	Region originalRegion = getCoarsener()
		.translateCoarsenedToOriginalRegion(coarsenedRegion);
	return originalRegion.getHalfPerimeter();
    }

    @Override
    protected int getWeightLowerBound(JoinMatrix coarsenedMatrix,
	    int numOfRegions) {
	/*
	 * old version int numOutputs = coarsenedMatrix.getTotalNumOutputs();
	 * return (int)(2 * Math.sqrt(numOutputs/numOfRegions)); // we implied
	 * that JoinMatrix is 0/1, which is not necessarily the case
	 */
	// new version
	long numOutputs = coarsenedMatrix.getNumElements();
	return (int) (2 * Math.sqrt(numOutputs / numOfRegions));
    }

    @Override
    protected int getWeightUpperBound(JoinMatrix coarsenedMatrix,
	    int numOfRegions) {
	int xSize = getCoarsener().getOriginalXSize();
	int ySize = getCoarsener().getOriginalYSize();
	return xSize + ySize;
    }

    @Override
    public String toString() {
	return "OkcanExactInputAlgorithm" + "[" + super.toString() + "]";
    }

    @Override
    public WeightPrecomputation getPrecomputation() {
	return null;
    }

    @Override
    public WeightFunction getWeightFunction() {
	return _wf;
    }

    @Override
    public String getShortName() {
	return "oei";
    }
}