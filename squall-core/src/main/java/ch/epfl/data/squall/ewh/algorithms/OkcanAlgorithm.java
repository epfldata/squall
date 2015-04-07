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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.ewh.data_structures.BooleanRegions;
import ch.epfl.data.squall.ewh.data_structures.ExtremePositions;
import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.OverweightedException;
import ch.epfl.data.squall.ewh.data_structures.Region;
import ch.epfl.data.squall.ewh.data_structures.TooSmallMaxWeightException;
import ch.epfl.data.squall.ewh.main.PushStatisticCollector;
import ch.epfl.data.squall.utilities.MyUtilities;

public abstract class OkcanAlgorithm implements TilingAlgorithm {
	private static class BooleanRegion {
		private boolean _satisfied;
		private Region _region;

		public BooleanRegion(boolean satisfied, Region region) {
			_satisfied = satisfied;
			_region = region;
		}

		public Region getRegion() {
			return _region;
		}

		public boolean isSatisfied() {
			return _satisfied;
		}

		public void setRegion(Region region) {
			_region = region;
		}

		public void setSatisfied(boolean satisfied) {
			_satisfied = satisfied;
		}
	}

	private static class CurrentRowRegions {
		private int _currentRow;
		private List<Region> _regions;

		public CurrentRowRegions(int currentRow, List<Region> regions) {
			_currentRow = currentRow;
			_regions = regions;
		}

		public int getCurrentRow() {
			return _currentRow;
		}

		public List<Region> getRegions() {
			return _regions;
		}

		public void setCurrentRow(int currentRow) {
			_currentRow = currentRow;
		}

		public void setRegions(List<Region> regions) {
			_regions = regions;
		}
	}

	private static int getSumOfFrequencies(List<Region> regions) {
		int freq = 0;
		for (Region region : regions) {
			freq += region.getFrequency();
		}
		return freq;
	}

	private static Region mergeRegions(Region originalRegion, Region newRegion) {
		Region result = new Region(originalRegion);
		int r_x1 = result.get_x1();
		int r_y1 = result.get_y1();
		int r_x2 = result.get_x2();
		int r_y2 = result.get_y2();
		int r_freq = result.getFrequency();

		int n_x1 = newRegion.get_x1();
		int n_y1 = newRegion.get_y1();
		int n_x2 = newRegion.get_x2();
		int n_y2 = newRegion.get_y2(); // the same as c_y1
		int n_freq = newRegion.getFrequency();

		if (n_x1 < r_x1) {
			// move upper left corner up
			result.set_x1(n_x1);
		}
		if (n_y1 < r_y1) {
			// move upper left corner left
			result.set_y1(n_y1);
		}
		if (n_x2 > r_x2) {
			// move lower right corner down
			result.set_x2(n_x2);
		}
		if (n_y2 > r_y2) {
			// move lower right corner right
			result.set_y2(n_y2);
		}

		result.setFrequency(r_freq + n_freq); // TODO for this to work, the
												// regions have to be
												// non-overlapping

		return result;
	}

	private static final double CLOSE_PERCENTAGE = 0.05;

	private static Logger LOG = Logger
			.getLogger(OkcanCandidateInputAlgorithm.class);
	private int _numXBuckets, _numYBuckets;

	private OkcanCoarsener _coarsener;

	private JoinMatrix _originalMatrix;
	private Map _map;

	private int _j;
	private StringBuilder _sb;

	// for monotonic joins: assumes a row has at least one candidate cell
	private HashMap<Integer, ExtremePositions> _rowExtremes;

	private HashMap<Integer, ExtremePositions> _columnExtremes;

	private boolean _isMonotonic = true;

	private boolean _isExact = false;

	public OkcanAlgorithm(int j, int numXBuckets, int numYBuckets, Map map,
			OkcanCoarsener coarsener) {
		_map = map;
		_j = j;

		_numXBuckets = numXBuckets;
		_numYBuckets = numYBuckets;
		_coarsener = coarsener;
		if (_coarsener instanceof OkcanExactCoarsener) {
			_isExact = true;
			LOG.info("Setting exactCoarsener is less efficient!");
		}
	}

	private List<Region> binarySearch(int lowerBound, int upperBound,
			int numOfRegions, JoinMatrix coarsenedMatrix) {
		List<Region> result = null;

		/*
		 * this binary search will find the smallest maxInput in [lowerBound,
		 * upperBound] such that mBucket can perform tiling with at most
		 * numOfRegions rectangles It is guaranteed to find the smallest
		 * maxInput, as in the case of success, we always go left in the range
		 */
		while (lowerBound <= upperBound) {
			int middle = (lowerBound + upperBound) / 2;
			LOG.info("New binary search with middle = " + middle);
			if (middle == 0 && lowerBound == 0 && upperBound == 1) {
				// middle should never be 0
				lowerBound++;
				continue;
			}

			// to save same time, we do not try every single possibility for
			// large lowerBound, upperBound
			if ((result != null)
					&& (MyUtilities.computePercentage(lowerBound, upperBound) < CLOSE_PERCENTAGE)) {
				LOG.info("Terminated binary search for lowerBound = "
						+ lowerBound + ", upperBound = " + upperBound);
				break;
			}

			_sb.append("\nAt time ")
					.append(PushStatisticCollector.getWallClockTime())
					.append("\n");
			_sb.append("Binary search middle = ").append(middle).append("\n");
			BooleanRegions br = mBucket(middle, numOfRegions, coarsenedMatrix);
			if (br.isSatisfied()) {
				// keep the last good result
				if (br.getRegions() != null) {
					// to avoid corner case when there are 0 regions
					result = br.getRegions();
				}
				// if it is possible to build numOfRegions rectangles with
				// maxInput = middle, try with smaller maxInput
				upperBound = middle - 1;
			} else {
				// let's try with bigger maxInput
				lowerBound = middle + 1;
			}
		}
		return result;
	}

	/*
	 * Covers a block of consecutive rows by partitioning them by columns First
	 * and last row are inclusive
	 */
	private List<Region> coverRows(int firstRow, int lastRow, int maxWeight,
			JoinMatrix coarsenedMatrix) throws OverweightedException {
		List<Region> regions = new ArrayList<Region>();
		Region currentRegion = null;
		boolean regionStarted = false;

		int startYIndex = 0;
		int endYIndex = coarsenedMatrix.getYSize(); // non-inclusive
		if (_isMonotonic) {
			startYIndex = _rowExtremes.get(firstRow).getMostLeft();
			endYIndex = _rowExtremes.get(lastRow).getMostRight() + 1; // +1
																		// because
																		// endIndex
																		// is
																		// non-inclusive
		}

		// my optimization: instead of going one by one, we could use convex
		// optimization to reduce running time
		// not implemented because that's not written in their paper (that's my
		// idea)
		// also not sure if it would bring performance benefits
		for (int j = startYIndex; j < endYIndex; j++) {
			BooleanRegion candidateColumn = getOuterColumnCandidates(firstRow,
					lastRow, j, coarsenedMatrix);
			if (candidateColumn.isSatisfied()) {// isCandidate
				Region candidateColumnRegion = candidateColumn.getRegion();
				if (!regionStarted) {
					currentRegion = candidateColumnRegion;
					regionStarted = true;
					if (getWeight(currentRegion) > maxWeight) {
						// one line weight is bigger than maximum allowed weight
						throw new OverweightedException(maxWeight, firstRow, j,
								lastRow, j);
					}
				} else {
					Region extendedRegion = mergeRegions(currentRegion,
							candidateColumnRegion);
					if (getWeight(extendedRegion) <= maxWeight) {
						currentRegion = extendedRegion;
					} else {
						regions.add(currentRegion);
						j--; // go over this index again
						regionStarted = false;
					}
				}
			}
		}

		if (regionStarted) {
			// the last region which contains some candidate cells, but did not
			// reach maxInput
			regions.add(currentRegion);
		}
		if (regions.isEmpty()) {
			regions = null;
		}

		return regions;
	}

	/*
	 * Covers "the best" block of consecutive rows
	 */
	private CurrentRowRegions coverSubMatrix(int currentRow, int maxWeight,
			JoinMatrix coarsenedMatrix) throws TooSmallMaxWeightException {
		double maxScore = -1;
		// we may not find even a single candidate cell in the subMatrix
		int bestRow = currentRow + maxWeight - 1;
		List<Region> bestRegions = null;

		try {
			for (int i = 0; i < maxWeight; i++) { // 0 as there could be only
													// one row left in the
													// matrix to be processed
				// check if lastRow is out of scope
				if (currentRow + i >= coarsenedMatrix.getXSize())
					break;

				List<Region> regions = coverRows(currentRow, currentRow + i,
						maxWeight, coarsenedMatrix);
				if (regions != null) {
					// there is at least one candidate cell, so that at least
					// one region is created
					double area = getSumOfFrequencies(regions);
					double score = area / regions.size();
					if (score >= maxScore) {
						maxScore = score;
						bestRow = currentRow + i;
						bestRegions = regions;
					}
				}
			}
		} catch (OverweightedException oe) {
			// one column was too heavy in terms of weight
			// does not make sense to try with even taller columns
			// has the effect of break from the loop
			LOG.info("OverweightedColumn " + oe);

			// we were not able to make any coverage; because of
			// OverweightedException, we know there is something to be covered
			if (bestRegions == null) {
				throw new TooSmallMaxWeightException(maxScore, null);
			}
		}

		return new CurrentRowRegions(bestRow + 1, bestRegions);
	}

	private List<Region> getCoarsenedRegions(JoinMatrix coarsenedMatrix,
			int numOfRegions) {
		int lowerBound = getWeightLowerBound(coarsenedMatrix, numOfRegions);
		int upperBound = getWeightUpperBound(coarsenedMatrix, numOfRegions);

		return binarySearch(lowerBound, upperBound, numOfRegions,
				coarsenedMatrix);
	}

	protected OkcanCoarsener getCoarsener() {
		return _coarsener;
	}

	/*
	 * All the boundaries are inclusive
	 */
	private BooleanRegion getOuterColumnCandidates(int firstRow, int lastRow,
			int column, JoinMatrix coarsenedMatrix) {
		if (!_isMonotonic) {
			boolean isCandidate = false;
			Region region = null;
			int firstCandRow = -1;
			int lastCandRow = -1;
			int frequency = 0;

			for (int i = firstRow; i <= lastRow; i++) {
				int numOutputs = coarsenedMatrix.getElement(i, column);
				if (numOutputs > 0) {
					// first is set only first time, last is set each time
					if (!isCandidate) {
						firstCandRow = i;
						isCandidate = true;
					}
					lastCandRow = i;
					frequency += numOutputs;
				}
			}

			if (isCandidate) {
				region = new Region(firstCandRow, column, lastCandRow, column,
						frequency);
			}
			return new BooleanRegion(isCandidate, region);
		} else {
			// monotonic case
			ExtremePositions epc = _columnExtremes.get(column);
			int firstCandRow = epc.getMostLeft();
			int lastCandRow = epc.getMostRight();
			firstRow = MyUtilities.getMax(firstRow, firstCandRow);
			lastRow = MyUtilities.getMin(lastRow, lastCandRow);
			Region region = null;
			boolean isCandidate = false;

			if (firstRow <= lastRow) {
				isCandidate = true;
				int frequency = 0;
				if (_isExact) {
					for (int i = firstRow; i <= lastRow; i++) {
						// needs to sum up actual values
						frequency += coarsenedMatrix.getElement(i, column);
					}
				} else {
					frequency += (lastRow - firstRow + 1);
				}
				/*
				 * old version of the code for(int i = firstRow; i <= lastRow;
				 * i++){ if(_isExact){ // needs to sum up actual values
				 * frequency += coarsenedMatrix.getElement(i, column); }else{ //
				 * all the values in the range are ones // no need for
				 * coarsenedMatrix at all frequency++; } }
				 */
				region = new Region(firstRow, column, lastRow, column,
						frequency);
			}
			return new BooleanRegion(isCandidate, region);
		}
	}

	/*
	 * Minimum possible weight for a region; used in binary search
	 */
	protected abstract int getWeightLowerBound(JoinMatrix coarsenedMatrix,
			int numOfRegions);

	/*
	 * Maximum possible weight for a region; used in binary search
	 */
	protected abstract int getWeightUpperBound(JoinMatrix coarsenedMatrix,
			int numOfRegions);

	/*
	 * Heuristics to cover matrix with numOfRegion regions and maxInput maximum
	 * input (half-perimeter)
	 */
	private BooleanRegions mBucket(int maxWeight, int numOfRegions,
			JoinMatrix coarsenedMatrix) {
		try {
			int currentRow = 0;
			List<Region> allRegions = null;
			while (currentRow < coarsenedMatrix.getXSize()) {
				CurrentRowRegions rr = coverSubMatrix(currentRow, maxWeight,
						coarsenedMatrix);
				currentRow = rr.getCurrentRow();
				List<Region> newRegions = rr.getRegions();

				if (newRegions != null) {
					// subMatrix has at least one candidate cell, and thus, at
					// least one region
					if (allRegions == null) {
						allRegions = new ArrayList<Region>();
					}
					allRegions.addAll(newRegions);

					if (allRegions.size() > numOfRegions) {
						return new BooleanRegions(false, allRegions);
					}
				}
			}
			return new BooleanRegions(true, allRegions);
		} catch (TooSmallMaxWeightException e) {
			LOG.info("TooSmallWeightException " + e);
			return new BooleanRegions(false, null);
		}
	}

	// TODO: As the authors said in Section 5.1, we may align bucket boundaries,
	// as this is beneficial for "some queries"
	@Override
	public List<Region> partition(JoinMatrix joinMatrix, StringBuilder sb) {
		_sb = sb;
		// computing bucket sizes
		_originalMatrix = joinMatrix;
		int xSize = joinMatrix.getXSize();
		int ySize = joinMatrix.getYSize();

		// there cannot be more buckets than elements in any of the relations
		if (_numXBuckets > xSize) {
			_numXBuckets = xSize;
			_sb.append("WARNING: Bucket size X reduced to the number of rows ")
					.append(xSize).append("\n");
		}
		if (_numYBuckets > ySize) {
			_numYBuckets = ySize;
			_sb.append(
					"WARNING: Bucket size Y reduced to the number of columns ")
					.append(ySize).append("\n");
		}

		// creation of coarsenedMatrix
		JoinMatrix coarsenedMatrix = _coarsener.createAndFillCoarsenedMatrix(
				joinMatrix, _numXBuckets, _numYBuckets, _map);
		LOG.info("Created coarsened matrix in OkcanAlgorithm.");

		if (_isMonotonic) {
			precomputeRowExtremes();
			precomputeColumnExtremes();
		}

		LOG.info("Precomputed row extremes in OkcanAlgorithm.");

		long candidateGridCells = coarsenedMatrix.getNumElements();
		LOG.info("The number of candidate cells in the coarsenedMatrix in OkcanAlgorithm is "
				+ candidateGridCells);
		_sb.append("\nThe number of candidate grid cells is ")
				.append(candidateGridCells).append(".\n");
		// check if the number of candidate grid cells is smaller than the
		// number of joiners
		if (candidateGridCells < _j) {
			// one joiner must have at least one cell
			throw new RuntimeException(
					"Too coarse-grained partitioning, not enough cells!");
		}

		LOG.info("Started binary search in OkcanAlgorithm.");
		// actual work
		List<Region> coarsenedRegions = getCoarsenedRegions(coarsenedMatrix, _j);
		return _coarsener.translateCoarsenedToOriginalRegions(coarsenedRegions);
	}

	private void precomputeColumnExtremes() {
		_columnExtremes = new HashMap<Integer, ExtremePositions>();

		int firstCandInLastColumn = 0;
		for (int j = 0; j < _numYBuckets; j++) {
			boolean isFirstInColumn = true;
			int y1 = _coarsener.getOriginalYCoordinate(j, false);
			int y2 = _coarsener.getOriginalYCoordinate(j, true);
			for (int i = firstCandInLastColumn; i < _numXBuckets; i++) {
				int x1 = _coarsener.getOriginalXCoordinate(i, false);
				int x2 = _coarsener.getOriginalXCoordinate(i, true);
				// LOG.info("x1 = " + x1 + ", y1 = " + y1 + ", x2 = " + x2 +
				// ", y2 = " + y2);
				Region region = new Region(x1, y1, x2, y2);
				boolean isCandidate = MyUtilities.isCandidateRegion(
						_originalMatrix, region,
						_originalMatrix.getComparisonPredicate(), _map);
				if (isCandidate) {
					if (isFirstInColumn) {
						firstCandInLastColumn = i;
						ExtremePositions ep = new ExtremePositions(i, i);
						_columnExtremes.put(j, ep);
						isFirstInColumn = false;
					} else {
						ExtremePositions ep = _columnExtremes.get(j);
						ep.setMostRight(i); // update the last position (i) with
											// value j in place
					}
				}
				if (!isFirstInColumn && !isCandidate) {
					// I am right from the candidate are; the first
					// non-candidate guy means I should switch to the next row
					break;
				}
			}
		}
	}

	// take advantage of monotonicity; necessary for performance reasons
	// We should put this in OkcanCoarsener, but it's interface
	// We cannot put it in JoinMatrix (create a method similar to
	// joinMatrix.getNumCandidatesIterate)
	// because coarsenerMatrix JoinMatrix has no joinAttributes set.
	// Coordinates are in terms of coarsenedMatrix
	private void precomputeRowExtremes() {
		_rowExtremes = new HashMap<Integer, ExtremePositions>();

		int firstCandInLastRow = 0;
		for (int i = 0; i < _numXBuckets; i++) {
			boolean isFirstInRow = true;
			int x1 = _coarsener.getOriginalXCoordinate(i, false);
			int x2 = _coarsener.getOriginalXCoordinate(i, true);
			for (int j = firstCandInLastRow; j < _numYBuckets; j++) {
				int y1 = _coarsener.getOriginalYCoordinate(j, false);
				int y2 = _coarsener.getOriginalYCoordinate(j, true);
				// LOG.info("x1 = " + x1 + ", y1 = " + y1 + ", x2 = " + x2 +
				// ", y2 = " + y2);
				Region region = new Region(x1, y1, x2, y2);
				boolean isCandidate = MyUtilities.isCandidateRegion(
						_originalMatrix, region,
						_originalMatrix.getComparisonPredicate(), _map);
				if (isCandidate) {
					if (isFirstInRow) {
						firstCandInLastRow = j;
						ExtremePositions ep = new ExtremePositions(j, j);
						_rowExtremes.put(i, ep);
						isFirstInRow = false;
					} else {
						ExtremePositions ep = _rowExtremes.get(i);
						ep.setMostRight(j); // update the last position (i) with
											// value j in place
					}
				}
				if (!isFirstInRow && !isCandidate) {
					// I am right from the candidate are; the first
					// non-candidate guy means I should switch to the next row
					break;
				}
			}
		}
	}

	@Override
	public String toString() {
		return "numXBuckets = " + _numXBuckets + ", numYBuckets = "
				+ _numYBuckets;
	}
}