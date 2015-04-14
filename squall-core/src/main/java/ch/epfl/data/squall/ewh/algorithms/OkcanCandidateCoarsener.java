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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.Region;
import ch.epfl.data.squall.ewh.data_structures.UJMPAdapterByteMatrix;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.utilities.MyUtilities;

public class OkcanCandidateCoarsener implements OkcanCoarsener {
    private static Logger LOG = Logger.getLogger(OkcanCandidateCoarsener.class);

    protected JoinMatrix _originalMatrix;
    private int _originalXSize;
    private int _originalYSize;
    protected ComparisonPredicate _cp;
    protected Map _map;

    protected int _numXBuckets, _numYBuckets;
    private int _bucketXSize, _bucketYSize; // last bucket is slightly bigger

    protected void setParameters(JoinMatrix originalMatrix, int numXBuckets,
	    int numYBuckets, Map map) {
	_originalMatrix = originalMatrix;
	_originalXSize = _originalMatrix.getXSize();
	_originalYSize = _originalMatrix.getYSize();
	_cp = originalMatrix.getComparisonPredicate();
	_map = map;

	_numXBuckets = numXBuckets;
	_numYBuckets = numYBuckets;

	// compute bucket sizes: last bucket is slightly bigger
	_bucketXSize = _originalXSize / _numXBuckets;
	_bucketYSize = _originalYSize / _numYBuckets;

	LOG.info("OkcanCandidateCoarsener: _originalXSize = " + _originalXSize
		+ ", _originalYSize = " + _originalYSize);
	LOG.info("OkcanCandidateCoarsener: _numXBuckets = " + _numXBuckets
		+ ", _numYBuckets = " + _numYBuckets);
	LOG.info("Should be 1 when sampling is used: _bucketXSize = "
		+ _bucketXSize + ", _bucketYSize = " + _bucketYSize);
    }

    @Override
    public JoinMatrix createAndFillCoarsenedMatrix(JoinMatrix originalMatrix,
	    int numXBuckets, int numYBuckets, Map map) {
	setParameters(originalMatrix, numXBuckets, numYBuckets, map);

	JoinMatrix coarsenedMatrix = new UJMPAdapterByteMatrix(_numXBuckets,
		_numYBuckets, _map);
	int numElements = 0;
	long capacity = coarsenedMatrix.getCapacity();
	LOG.info("Capacity of coarsened joinMatrix in OkcanCandidateCoarsener is "
		+ capacity);

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
		    coarsenedMatrix.setElement(1, i, j);
		    numElements++;
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

	if (numElements > capacity) {
	    throw new RuntimeException("Matrix size " + numElements
		    + " cannot surpass matrix capacity " + capacity);
	}

	return coarsenedMatrix;
    }

    @Override
    public List<Region> translateCoarsenedToOriginalRegions(
	    List<Region> coarsenedRegions) {
	List<Region> result = null;
	if (coarsenedRegions != null) {
	    result = new ArrayList<Region>();
	}

	// LOG.info("Bucketized regions are " + Region.toString(bucketRegions,
	// null));

	for (Region coarsenedRegion : coarsenedRegions) {
	    Region region = translateCoarsenedToOriginalRegion(coarsenedRegion);
	    result.add(region);
	}
	return result;
    }

    @Override
    public Region translateCoarsenedToOriginalRegion(Region coarsenedRegion) {
	int cx1 = coarsenedRegion.get_x1();
	int cy1 = coarsenedRegion.get_y1();
	int cx2 = coarsenedRegion.get_x2();
	int cy2 = coarsenedRegion.get_y2();

	int x1 = getOriginalXCoordinate(cx1, false);
	int y1 = getOriginalYCoordinate(cy1, false);
	int x2 = getOriginalXCoordinate(cx2, true);
	int y2 = getOriginalYCoordinate(cy2, true);

	return new Region(x1, y1, x2, y2);
    }

    // the same as in InputShallowCoarsener
    @Override
    public int getOriginalXCoordinate(int cx, boolean isHigher) {
	// this point is included; we take end of this point as the boundary
	// this point is out of maths, as the last bucket can be larger
	if (cx == _numXBuckets - 1) {
	    if (isHigher) {
		return _originalXSize - 1;
	    }
	}

	// non-last bucket
	if (isHigher) {
	    // the end of this bucket is the boundary
	    cx++;
	}
	int x = cx * _bucketXSize;
	if (isHigher) {
	    // the end of this bucket is the boundary
	    x--;
	}

	return x;
    }

    @Override
    public int getOriginalYCoordinate(int cy, boolean isHigher) {
	// this point is included; we take end of this point as the boundary
	// this point is out of maths, as the last bucket can be larger
	if (cy == _numYBuckets - 1) {
	    if (isHigher) {
		return _originalYSize - 1;
	    }
	}

	// non-last bucket
	if (isHigher) {
	    // the end of this bucket is the boundary
	    cy++;
	}
	int y = cy * _bucketYSize;
	if (isHigher) {
	    // the end of this bucket is the boundary
	    y--;
	}

	return y;
    }

    @Override
    public int getOriginalXSize() {
	return _originalXSize;
    }

    @Override
    public int getOriginalYSize() {
	return _originalYSize;
    }

    /*
     * // Old versions // The same as in InputIntCoarsener public List<Region>
     * translateCoarsenedToOriginalRegions(List<Region> bucketRegions) {
     * List<Region> result = null; if(bucketRegions != null){ result = new
     * ArrayList<Region>(); }
     * 
     * for(Region bucketRegion: bucketRegions){ Point upperLeft =
     * bucketRegion.getCorner(0); Point lowerRight = bucketRegion.getCorner(3);
     * Point origUpperLeft = translateCoarsenedToOriginalPoint(upperLeft,
     * false); Point origLowerRight =
     * translateCoarsenedToOriginalPoint(lowerRight, true);
     * 
     * // creating and adding regions Region region = new Region(origUpperLeft,
     * origLowerRight); result.add(region); } return result; }
     * 
     * private Point translateCoarsenedToOriginalPoint(Point orig, boolean
     * isBottomRight){ int cx = orig.get_x(); int cy = orig.get_y();
     * 
     * // this point is included; we take end of this point as the boundary
     * if(isBottomRight){ cx++; cy++; }
     * 
     * int x = cx * _bucketXSize; int y = cy * _bucketYSize;
     * 
     * // account for the last bucket, which could be bigger if(cx ==
     * _numXBuckets - 1){ x = _originalXSize - 1; } if(cy == _numYBuckets -1){ y
     * = _originalYSize - 1; }
     * 
     * return new Point (x,y); }
     * 
     * 
     * //------------------------------------------------------------------------
     * ------------------------------------------
     * 
     * // Old costly variant of finding the candidates
     * 
     * public JoinBooleanMatrixInterface createAndFillCoarsenedMatrix() {
     * JoinBooleanMatrixInterface coarsenedMatrix = new
     * UJMPAdapterByteMatrix(_numXBuckets, _numYBuckets, _map); for(int i = 0; i
     * < _numXBuckets; i++){ for (int j = 0; j < _numYBuckets; j++){
     * if(isCandidateRegion(_originalMatrix, i, j, _bucketXSize, _bucketYSize)){
     * coarsenedMatrix.setElement(true, i, j); } } } return coarsenedMatrix; }
     * 
     * //The regions are fixed, and for each region, this method is called
     * exactly once private boolean isCandidateRegion(JoinBooleanMatrixInterface
     * joinMatrix, int bucketI, int bucketJ, int bucketXSize, int bucketYSize){
     * int upperI = (bucketI+1) * bucketXSize; int upperJ = (bucketJ+1) *
     * bucketYSize;
     * 
     * // last buckets can be bigger if(bucketI == _numXBuckets - 1){ upperI =
     * joinMatrix.getXSize(); } if(bucketJ == _numYBuckets - 1){ upperJ =
     * joinMatrix.getYSize(); }
     * 
     * for (int i = bucketI * bucketXSize; i < upperI; i++){ for (int j =
     * bucketJ * bucketYSize; j < upperJ; j++){ if(joinMatrix.getElement(i, j)){
     * // a region is candidate if it contains at least one candidate cell
     * return true; } } } return false; }
     */
}