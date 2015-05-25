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

import org.apache.log4j.Logger;

import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.data_structures.Region;
import ch.epfl.data.squall.utilities.MyUtilities;

public class InputOutputShallowCoarsener extends OutputShallowCoarsener {
    private static Logger LOG = Logger
	    .getLogger(InputOutputShallowCoarsener.class);

    public InputOutputShallowCoarsener(int numXBuckets, int numYBuckets,
	    WeightFunction wf, Map map, ADJUST_MODE amode, ITER_MODE imode) {
	super(numXBuckets, numYBuckets, wf, map, amode, imode);
    }

    public InputOutputShallowCoarsener(int numXBuckets, int numYBuckets,
	    WeightFunction wf, Map map) {
	super(numXBuckets, numYBuckets, wf, map);
    }

    // the following methods I might want to overload
    // weight = a * input + b * output
    @Override
    protected double computeMaxGridCellWeight() {
	// in the worst case, maxGridCellWeight is
	// * for pxp partitioning, m/p
	// * for pxq partitioning, m/(min(p,q))
	int minSideLength = findMin(_numXBuckets, _numYBuckets);
	int totalNumOutputs = _originalMatrix.getTotalNumOutputs();
	int frequency = totalNumOutputs / minSideLength;
	int halfPerimeter = _originalMatrix.getXSize() / _numXBuckets
		+ _originalMatrix.getYSize() / _numYBuckets;

	double result = MAX_WEIGHT_BADNESS * (2 + EPSILON)
		* _wf.getWeight(halfPerimeter, frequency);
	// LOG.info("Max grid cell weight is " + result);
	return result;
    }

    // weight = a * input + b * output
    @Override
    protected double getWeight(Region region) {
	return _wp.getWeight(region);
    }

//    // returns 0 if the region contains no output
//    @Override
//    protected double getWeightEmpty0(Region region) {
//	// too expensive and does not yield better partitioning than only
//	// "return _wp.getWeight(region);" (tried on one example though):
//	if (_wp.getFrequency(region) == 0
//		&& !MyUtilities.isCandidateRegion(_originalMatrix, region,
//			_originalMatrix.getComparisonPredicate(), _map)) {
//	    // if (_wp.getFrequency(region) == 0){
//	    // input can be arbitrarily large if it contains no output
//	    // Would it be an improvement if we assign non-zero weights for
//	    // empty regions which are candidates?
//	    return 0;
//	} else {
//	    return _wp.getWeight(region);
//	}
//    }
    
//    //optimized version1
//    // returns 0 if the region contains no output
//    @Override
//    protected double getWeightEmpty0(Region region) {
//    	int frequency = _wp.getFrequency(region); 
//    	if (frequency == 0
//    			&& !MyUtilities.isCandidateRegion(_originalMatrix, region,
//    					_originalMatrix.getComparisonPredicate(), _map)) {
			//input can be arbitrarily large if it contains no output
			//Would it be an improvement if we assign non-zero weights for
			//empty regions which are candidates?    
//    		return 0;
//    	} else {
//    		return _wp.getWeightFunction().getWeight(region.getHalfPerimeter(), frequency);
//    	}
//    }
    
    //optimized version2
    // returns 0 if the region contains no output
    @Override
    protected double getWeightEmpty0(Region region) {
    	if (!MyUtilities.isCandidateRegion(_originalMatrix, region,
    					_originalMatrix.getComparisonPredicate(), _map)) {
    		//If a cell is not a candidate, there should be no output samples
    		return 0;
			//input can be arbitrarily large if it contains no output
			//Would it be an improvement if we assign non-zero weights for
			//empty regions which are candidates?    		
    	} else {
    		return _wp.getWeight(region);
    	}
    }

    @Override
    public String toString() {
	return "InputOutputBinaryCoarsener [numXBuckets = " + _numXBuckets
		+ ", numYBuckets = " + _numYBuckets + "]";
    }

}