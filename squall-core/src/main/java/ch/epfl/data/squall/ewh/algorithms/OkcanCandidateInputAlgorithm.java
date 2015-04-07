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


package ch.epfl.data.squall.ewh.algorithms;

import java.util.Map;

import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.Region;

public class OkcanCandidateInputAlgorithm extends OkcanAlgorithm {
	// that's how the optimality would be checked
	private WeightFunction _wf;

	public OkcanCandidateInputAlgorithm(int j, WeightFunction wf,
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
		return "oci";
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
		return coarsenedRegion.getHalfPerimeter();
	}

	@Override
	public WeightFunction getWeightFunction() {
		return _wf;
	}

	@Override
	protected int getWeightLowerBound(JoinMatrix coarsenedMatrix,
			int numOfRegions) {
		long numOutputs = coarsenedMatrix.getNumElements();
		return (int) (2 * Math.sqrt(numOutputs / numOfRegions));
	}

	@Override
	protected int getWeightUpperBound(JoinMatrix coarsenedMatrix,
			int numOfRegions) {
		int xSize = coarsenedMatrix.getXSize(); // this is SINGLE_D_BUCKETS
		int ySize = coarsenedMatrix.getYSize(); // this is SINGLE_D_BUCKETS
		// each region should have at least one candidate cell
		return xSize + ySize - 2 * (numOfRegions - 1);
	}

	@Override
	public String toString() {
		return "OkcanCandidateInputAlgorithm" + "[" + super.toString() + "]";
	}
}