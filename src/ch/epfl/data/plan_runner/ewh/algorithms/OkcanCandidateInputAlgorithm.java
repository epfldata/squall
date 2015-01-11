package ch.epfl.data.plan_runner.ewh.algorithms;

import java.util.Map;

import ch.epfl.data.plan_runner.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;

public class OkcanCandidateInputAlgorithm extends OkcanAlgorithm{
	// that's how the optimality would be checked
	private WeightFunction _wf;
	
	public OkcanCandidateInputAlgorithm(int j, WeightFunction wf, int numXBuckets, int numYBuckets, Map map){
		super(j, numXBuckets, numYBuckets, map, new OkcanCandidateCoarsener());
		_wf = wf;
	}
	
	// coarsened region
	@Override
	public double getWeight(Region coarsenedRegion) {
		// Currently, the actual input is at most maxInput * bucketSize of bigger relation 
		// TODO we should scale sides according to bucketXSize : bucketYSize
		//  but they don't do this in the paper
		//  This would require to change the bounds for binarySearch (increase the upperBound)  
		return coarsenedRegion.getHalfPerimeter();
	}
	
	@Override
	protected int getWeightLowerBound(JoinMatrix coarsenedMatrix, int numOfRegions) {
		long numOutputs = coarsenedMatrix.getNumElements();
		return (int)(2 * Math.sqrt(numOutputs/numOfRegions));
	}

	@Override
	protected int getWeightUpperBound(JoinMatrix coarsenedMatrix, int numOfRegions) {
		int xSize = coarsenedMatrix.getXSize(); // this is SINGLE_D_BUCKETS
		int ySize = coarsenedMatrix.getYSize(); // this is SINGLE_D_BUCKETS
		// each region should have at least one candidate cell
		return xSize + ySize - 2 * (numOfRegions - 1);
	}
	
	@Override
	public String toString(){
		return "OkcanCandidateInputAlgorithm" + "[" + super.toString() + "]";
	}

	@Override
	public WeightPrecomputation getPrecomputation() {
		return null;
	}
	
	@Override
	public WeightFunction getWeightFunction(){
		return _wf;
	}
	
	@Override
	public String getShortName() {
		return "oci";
	}
}