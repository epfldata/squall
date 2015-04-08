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

import java.util.List;

import ch.epfl.data.squall.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.Region;

public interface TilingAlgorithm {

    // sb is for collecting stats
    public List<Region> partition(JoinMatrix joinMatrix, StringBuilder sb);

    // returns null for those algorithm which do not use it (e.g. Okcan)
    public WeightPrecomputation getPrecomputation();

    // is always specified; that's what algorithm's optimality will be tested
    // against
    // not necessarily what it opimizes for (e.g. Okcan)
    public WeightFunction getWeightFunction();

    public double getWeight(Region region);

    public String getShortName();

}
