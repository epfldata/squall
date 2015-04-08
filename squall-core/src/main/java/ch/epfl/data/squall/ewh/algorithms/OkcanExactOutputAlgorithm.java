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