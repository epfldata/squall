package ch.epfl.data.plan_runner.ewh.algorithms.optimality;

import ch.epfl.data.plan_runner.ewh.data_structures.Region;

public interface OptimalityMetricInterface {

	/*
	 *  
	 * return value is better if it is lower
	 */
	public double getOptimalityDistance();
	
	// according to wf; regions are final(non-coarsened) regions
	public double getWeight(Region region);

	// actual maxRegionWeight from existing regions
	public double getActualMaxRegionWeight();
	
}
