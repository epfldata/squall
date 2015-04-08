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

import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.Region;
import ch.epfl.data.squall.ewh.data_structures.UJMPAdapterIntMatrix;
import ch.epfl.data.squall.utilities.MyUtilities;

// this class makes a hybrid between Okcan and us:
//   it uses Okcan heuristics, but keeps the exact information about the output
public class OkcanExactCoarsener extends OkcanCandidateCoarsener {
    private static Logger LOG = Logger.getLogger(OkcanExactCoarsener.class);

    @Override
    public JoinMatrix createAndFillCoarsenedMatrix(JoinMatrix originalMatrix,
	    int numXBuckets, int numYBuckets, Map map) {
	setParameters(originalMatrix, numXBuckets, numYBuckets, map);

	JoinMatrix coarsenedMatrix = new UJMPAdapterIntMatrix(_numXBuckets,
		_numYBuckets, _map);
	LOG.info("Capacity of coarsened joinMatrix in OkcanExactCoarsener is "
		+ coarsenedMatrix.getCapacity());
	int firstCandInLastLine = 0;
	for (int i = 0; i < _numXBuckets; i++) {
	    boolean isFirstInLine = true;
	    int x1 = getOriginalXCoordinate(i, false);
	    int x2 = getOriginalXCoordinate(i, true);
	    for (int j = firstCandInLastLine; j < _numYBuckets; j++) {
		int y1 = getOriginalYCoordinate(j, false);
		int y2 = getOriginalYCoordinate(j, true);
		// LOG.info("x1 = " + x1 + ", y1 = " + y1 + ", x2 = " + x2 +
		// ", y2 = " + y2);
		Region region = new Region(x1, y1, x2, y2);
		boolean isCandidate = MyUtilities.isCandidateRegion(
			originalMatrix, region, _cp, _map);
		if (isCandidate) {
		    int frequency = originalMatrix
			    .getRegionNumOutputs(new Region(x1, y1, x2, y2));
		    if (frequency == 0) {
			frequency = 1;
			// it's a candidate, so let's assign to it a
			// minPositiveValue
		    }
		    coarsenedMatrix.setElement(frequency, i, j);
		    if (isFirstInLine) {
			firstCandInLastLine = j;
			isFirstInLine = false;
		    }
		}
		if (!isFirstInLine && !isCandidate) {
		    // I am right from the candidate are; the first
		    // non-candidate guy means I should switch to the next row
		    break;
		}
	    }
	}
	return coarsenedMatrix;
    }
}
