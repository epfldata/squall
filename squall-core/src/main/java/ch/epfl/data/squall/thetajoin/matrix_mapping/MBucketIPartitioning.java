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


package ch.epfl.data.squall.thetajoin.matrix_mapping;

import java.util.ArrayList;

public class MBucketIPartitioning {

	public class Region {
		public int _row1, _col1, _row2, _col2;
		public int _capacity;
		public long _coveredArea = 0;
		private int _maxInput;

		public Region(int row1, int row2, int col, int maxInput,
				boolean[][] joinMatrix) {
			_row1 = row1;
			_row2 = row2;
			_col1 = col;
			_col2 = col;
			_maxInput = maxInput;
			_capacity = _row2 - _row1 + 1;
			for (int i = _row1; i <= _row2; i++)
				if (joinMatrix[i][_col1] == true)
					_coveredArea++;
		}

		public boolean incrementY(boolean[][] joinMatrix) {
			if (_capacity + 1 > _maxInput || _col2 + 1 >= joinMatrix[0].length)
				return false;
			// ELSE
			_col2++;
			_capacity++;
			for (int i = _row1; i <= _row2; i++)
				if (joinMatrix[i][_col2] == true)
					_coveredArea++;
			return true;
		}

		@Override
		public String toString() {
			return "Region with position: (" + _row1 + "," + _col1 + ")-->"
					+ "(" + _row2 + "," + _col2 + ") with capacity "
					+ _capacity + " and covered area " + _coveredArea;
		}
	}

	public static void main(String[] args) {

		boolean[][] joinMatrix = new boolean[4][4];

		joinMatrix[0][0] = true;
		joinMatrix[0][1] = true;
		joinMatrix[0][2] = false;
		joinMatrix[0][3] = false;
		joinMatrix[1][0] = true;
		joinMatrix[1][1] = true;
		joinMatrix[1][2] = false;
		joinMatrix[1][3] = false;
		joinMatrix[2][0] = false;
		joinMatrix[2][1] = false;
		joinMatrix[2][2] = true;
		joinMatrix[2][3] = true;
		joinMatrix[3][0] = false;
		joinMatrix[3][1] = false;
		joinMatrix[3][2] = true;
		joinMatrix[3][3] = true;

		MBucketIPartitioning mb = new MBucketIPartitioning(2, joinMatrix);
		mb.binarySearch();
		mb.printRegions();

	}

	private int _numWorkers;

	private boolean[][] _joinMatrix;

	private ArrayList<Region> _regions;

	public MBucketIPartitioning(int workers, boolean[][] joinMatrix) {
		_numWorkers = workers;
		_joinMatrix = joinMatrix;
	}

	public void binarySearch() {

		int maxLimit = _joinMatrix.length + _joinMatrix[0].length;
		int minLimit = (int) (2 * Math.sqrt(computeCoveredArea() / _numWorkers));

		// do the binary search
		binarySearch(minLimit, maxLimit);

	}

	private void binarySearch(int min, int max) {

		System.out.println("Binary Searching:(" + min + "," + max + ")");
		// if two elements in the end
		if (max - min == 1) {
			if (!process(max)) {
				process(min);
				System.out.println("Minimum is:" + min);
			} else
				System.out.println("Minimum is:" + max);
			return;
		}

		int pivot = (min + max) / 2;
		System.out.println("Pivot:" + pivot);
		if (process(pivot))
			binarySearch(min, pivot);
		else
			binarySearch(pivot, max);
	}

	private long computeCoveredArea() {
		int rows = _joinMatrix.length;
		int cols = _joinMatrix[0].length;
		long coveredArea = 0;
		for (int i = 0; i < rows; i++)
			for (int j = 0; j < cols; j++)
				if (_joinMatrix[i][j] == true)
					coveredArea++;
		return coveredArea;
	}

	private long computeCoveredArea(ArrayList<Region> regions) {
		long area = 0;
		for (int i = 0; i < regions.size(); i++)
			area += regions.get(i)._coveredArea;
		return area;
	}

	private ArrayList<Region> CoverRows(int row_s, int row_l, int maxInput,
			boolean[][] joinMatrix) {
		ArrayList<Region> regions = new ArrayList<MBucketIPartitioning.Region>();
		// Assuming everything is contigious
		int col_s = getFirstSetColumn(row_s, joinMatrix);
		Region r = new Region(row_s, row_l, col_s, maxInput, joinMatrix);

		for (int i = col_s; i < joinMatrix[0].length; i++) {
			if (!r.incrementY(joinMatrix)) {
				regions.add(r);
				r = new Region(row_s, row_l, i, maxInput, joinMatrix);
			}
		}
		regions.add(r);
		return regions;
	}

	private int[] coverSubmatrix(int row_s, int maxInput, int workers,
			boolean[][] joinMatrix) {
		int[] results = new int[2]; // first represents rows, and 2nd represents
		// workers
		long maxScore = -1;
		int bestRow = -2, rUsed = 0;
		ArrayList<Region> regions;
		ArrayList<Region> bestRegions = null;
		for (int i = 1; i < maxInput - 1; i++) {
			regions = CoverRows(row_s,
					Math.min(row_s + i, joinMatrix.length - 1), maxInput,
					joinMatrix);
			long area = computeCoveredArea(regions);
			long score = area / regions.size();
			if (score >= maxScore) {
				bestRow = row_s + i;
				rUsed = regions.size();
				bestRegions = regions;
			}
		}
		_regions.addAll(bestRegions);
		results[0] = bestRow + 1;
		results[1] = workers - rUsed;
		return results;
	}

	private int getFirstSetColumn(int row, boolean[][] joinMatrix) {
		for (int i = 0; i < joinMatrix[row].length; i++)
			if (joinMatrix[row][i] == true)
				return i;
		return -1;
	}

	private void printRegions() {
		for (int i = 0; i < _regions.size(); i++) {
			System.out.println(_regions.get(i).toString());
		}
	}

	private boolean process(int maxInput) {
		_regions = new ArrayList<Region>();
		int row = 0;
		int[] results;
		int workers = _numWorkers;
		while (row < _joinMatrix.length) {
			results = coverSubmatrix(row, maxInput, workers, _joinMatrix); // results=(row,r)
			row = results[0];
			workers = results[1];
			if (workers < 0)
				return false;
		}
		return true;
	}

}
