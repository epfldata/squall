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


package ch.epfl.data.squall.ewh.data_structures;

import ch.epfl.data.squall.ewh.algorithms.ShallowCoarsener;

public class TooSmallMaxWeightException extends Exception {
    private static final long serialVersionUID = 1L;
    private double _maxWeight;
    private ShallowCoarsener _coarsener;

    public TooSmallMaxWeightException(double maxWeight,
	    ShallowCoarsener coarsener) {
	_maxWeight = maxWeight;
	_coarsener = coarsener;
    }

    @Override
    public String toString() {
	return "Impossible to achieve maxWeight less than " + _maxWeight
		+ " with " + (_coarsener != null ? _coarsener : "");
    }

}