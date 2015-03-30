package ch.epfl.data.squall.ewh.algorithms;

import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.data_structures.Region;

public interface WeightPrecomputation {

	public int getFrequency(Region region);

	public int getMinHalfPerimeterForWeight(double maxWeight);

	public int getPrefixSum(int x, int y); // necessary for
	// PWeightPrecomputation

	public int getTotalFrequency();

	public double getWeight(Region region);

	public WeightFunction getWeightFunction();

	// prefix sum dimension
	public int getXSize();

	public int getYSize();

	// according to the matrix, not on the join condition
	// for join condition, look at Coarsener
	public boolean isEmpty(Region region);

}
