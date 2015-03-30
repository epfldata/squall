package ch.epfl.data.squall.ewh.algorithms;

import java.util.Map;

import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.Region;

public class OkcanCandidateOutputAlgorithm extends OkcanAlgorithm {
	// that's how the optimality would be checked
	private WeightFunction _wf;

	public OkcanCandidateOutputAlgorithm(int j, WeightFunction wf,
			int numXBuckets, int numYBuckets, Map map) {
		super(j, numXBuckets, numYBuckets, map, new OkcanCandidateCoarsener());
		_wf = wf;
	}

	@Override
	public WeightPrecomputation getPrecomputation() {
		return null;
	}

	@Override
	public String getShortName() {
		return "oco";
	}

	// coarsened region
	@Override
	public double getWeight(Region coarsenedRegion) {
		return coarsenedRegion.getFrequency();
	}

	@Override
	public WeightFunction getWeightFunction() {
		return _wf;
	}

	@Override
	protected int getWeightLowerBound(JoinMatrix coarsenedMatrix,
			int numOfRegions) {
		return (int) (coarsenedMatrix.getNumElements() / numOfRegions);
	}

	@Override
	protected int getWeightUpperBound(JoinMatrix coarsenedMatrix,
			int numOfRegions) {
		// each region should have at least one candidate cell
		return (int) (coarsenedMatrix.getNumElements() - (numOfRegions - 1));
	}

	@Override
	public String toString() {
		return "OkcanCandidateOutputAlgorithm" + "[" + super.toString() + "]";
	}
}