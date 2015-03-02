package plan_runner.ewh.algorithms;

import plan_runner.ewh.algorithms.optimality.WeightFunction;
import plan_runner.ewh.data_structures.Region;

public interface WeightPrecomputation {

	public WeightFunction getWeightFunction();
	
	// prefix sum dimension
	public int getXSize();
	
	public int getYSize();
	
	public double getWeight(Region region);
	
	public int getFrequency(Region region);
	
	//according to the matrix, not on the join condition
	// for join condition, look at Coarsener
	public boolean isEmpty(Region region);

	public int getTotalFrequency();

	public int getMinHalfPerimeterForWeight(double maxWeight);

	public int getPrefixSum(int x, int y); // necessary for PWeightPrecomputation
	
}
