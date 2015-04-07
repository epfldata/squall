/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ch.epfl.data.squall.ewh.algorithms.optimality;

import java.util.List;

import ch.epfl.data.squall.ewh.algorithms.WeightPrecomputation;
import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.Region;

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

	private MaxAvgOptimality(JoinMatrix joinMatrix, List<Region> regions) {
		_joinMatrix = joinMatrix;
		_regions = regions;
	}

	public MaxAvgOptimality(JoinMatrix joinMatrix, List<Region> regions,
			WeightFunction wf) {
		this(joinMatrix, regions);

		_wf = wf;
	}

	public MaxAvgOptimality(JoinMatrix joinMatrix, List<Region> regions,
			WeightPrecomputation wp) {
		this(joinMatrix, regions);
		_wp = wp;
	}

	@Override
	public double getActualMaxRegionWeight() {
		double maxWeight = 0;
		for (Region region : _regions) {
			double currentWeight = getWeight(region);
			if (currentWeight > maxWeight) {
				maxWeight = currentWeight;
			}
		}
		return maxWeight;
	}

	@Override
	public double getOptimalityDistance() {
		double maxWeight = 0;
		double totalWeight = 0;
		int numRegions = _regions.size();
		for (Region region : _regions) {
			double currentWeight = getWeight(region);
			totalWeight += currentWeight;
			if (currentWeight > maxWeight) {
				maxWeight = currentWeight;
			}
		}
		double avgWeight = totalWeight / numRegions;
		return maxWeight / avgWeight;
	}

	// we do not want to build WeightPrecomputation unless it already exists
	// cheaper to do getRegionNumOutputs, unless we have
	// SparseWeightPrecomputation
	// according to wf; regions are final(non-coarsened) regions
	@Override
	public double getWeight(Region region) {
		if (_wp != null) {
			return _wp.getWeight(region);
		} else {
			// a slower way, implies that joinMatrix contains exact output
			// information
			// only OkcanAlgorithm uses it
			return _wf.getWeight(region.getHalfPerimeter(),
					_joinMatrix.getRegionNumOutputs(region));
		}
	}

	@Override
	public String toString() {
		String weightString = "";
		if (_wp != null) {
			weightString = _wp.toString();
		} else {
			weightString = _wf.toString();
		}
		return "MAX(weight)/AVG(weight), where weight = " + weightString;
	}
}