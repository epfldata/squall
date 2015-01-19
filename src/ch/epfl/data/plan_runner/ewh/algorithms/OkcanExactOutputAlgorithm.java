package ch.epfl.data.plan_runner.ewh.algorithms;

import java.util.Map;

import ch.epfl.data.plan_runner.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;

public class OkcanExactOutputAlgorithm extends OkcanAlgorithm {
    // that's how the optimality would be checked
    private WeightFunction _wf;

    public OkcanExactOutputAlgorithm(int j, WeightFunction wf, int numXBuckets,
	    int numYBuckets, Map map) {
	super(j, numXBuckets, numYBuckets, map, new OkcanExactCoarsener());
	_wf = wf;
    }

    // coarsened region
    @Override
    public double getWeight(Region coarsenedRegion) {
	return coarsenedRegion.getFrequency();
    }

    @Override
    protected int getWeightLowerBound(JoinMatrix coarsenedMatrix,
	    int numOfRegions) {
	return coarsenedMatrix.getTotalNumOutputs() / numOfRegions;
    }

    @Override
    protected int getWeightUpperBound(JoinMatrix coarsenedMatrix,
	    int numOfRegions) {
	// each region should have at least one candidate cell
	return coarsenedMatrix.getTotalNumOutputs() - (numOfRegions - 1);
    }

    @Override
    public String toString() {
	return "OkcanExactOutputAlgorithm" + "[" + super.toString() + "]";
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
	return "oeo";
    }
}