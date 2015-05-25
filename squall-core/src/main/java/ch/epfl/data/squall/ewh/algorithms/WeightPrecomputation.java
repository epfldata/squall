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

import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.data_structures.Region;

public interface WeightPrecomputation {

    public WeightFunction getWeightFunction();

    // prefix sum dimension
    public int getXSize();

    public int getYSize();

    public double getWeight(Region region);

    public int getFrequency(Region region);
    
    // according to the matrix, not on the join condition
    // for join condition, look at Coarsener
    public boolean isEmpty(Region region);

    public int getTotalFrequency();

    public int getMinHalfPerimeterForWeight(double maxWeight);

    public int getPrefixSum(int x, int y); // necessary for
					   // PWeightPrecomputation

}