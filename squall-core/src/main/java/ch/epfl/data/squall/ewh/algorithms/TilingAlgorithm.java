package ch.epfl.data.squall.ewh.algorithms;

import java.util.List;

import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.Region;

public interface TilingAlgorithm {

	// returns null for those algorithm which do not use it (e.g. Okcan)
	public WeightPrecomputation getPrecomputation();

	public String getShortName();

	public double getWeight(Region region);

	// is always specified; that's what algorithm's optimality will be tested
	// against
	// not necessarily what it opimizes for (e.g. Okcan)
	public WeightFunction getWeightFunction();

	// sb is for collecting stats
	public List<Region> partition(JoinMatrix joinMatrix, StringBuilder sb);

}
