package ch.epfl.data.plan_runner.ewh.algorithms.optimality;

import java.util.List;

import ch.epfl.data.plan_runner.ewh.algorithms.WeightPrecomputation;
import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;

/*
 * MAX(weight)/AVG(weight)
 */
public class MaxAvgOptimality implements OptimalityMetricInterface {

	private JoinMatrix _joinMatrix;
	private List<Region> _regions;
	
	// either this is not null
	private WeightPrecomputation _wp;
	// or this is not null
	private WeightFunction _wf;
	
	private MaxAvgOptimality(JoinMatrix joinMatrix, List<Region> regions){
		_joinMatrix = joinMatrix;
		_regions = regions;
	}

	public MaxAvgOptimality(JoinMatrix joinMatrix, List<Region> regions, WeightFunction wf){
		this (joinMatrix, regions);
		
		_wf = wf;
	}
	
	public MaxAvgOptimality(JoinMatrix joinMatrix, List<Region> regions, WeightPrecomputation wp){
		this (joinMatrix, regions);
		_wp = wp;
	}
	
	@Override
	public double getOptimalityDistance() {
		double maxWeight = 0;
		double totalWeight = 0;
		int numRegions = _regions.size();
		for(Region region: _regions){
			double currentWeight = getWeight(region);
			totalWeight += currentWeight;
			if(currentWeight > maxWeight){
				maxWeight = currentWeight;
			}
		}
		double avgWeight = totalWeight/numRegions;
		return maxWeight/avgWeight;
	}
	
	@Override
	public String toString(){
		String weightString = "";
		if(_wp != null){
			weightString = _wp.toString();
		}else{
			weightString = _wf.toString();
		}
		return "MAX(weight)/AVG(weight), where weight = " + weightString;
	}
	
	// we do not want to build WeightPrecomputation unless it already exists
	// cheaper to do getRegionNumOutputs, unless we have SparseWeightPrecomputation
	//     according to wf; regions are final(non-coarsened) regions
	@Override
	public double getWeight(Region region){
		if(_wp != null){
			return _wp.getWeight(region);
		}else{
			// a slower way, implies that joinMatrix contains exact output information
			//only OkcanAlgorithm uses it
			return _wf.getWeight(region.getHalfPerimeter(), _joinMatrix.getRegionNumOutputs(region));
		}
	}

	@Override
	public double getActualMaxRegionWeight() {
		double maxWeight = 0;
		for(Region region: _regions){
			double currentWeight = getWeight(region); 
			if(currentWeight > maxWeight){
				maxWeight = currentWeight;
			}
		}
		return maxWeight;
	}
}