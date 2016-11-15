/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.thetajoin.matrix_assignment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

/**
 * Basically, it divides the join hypercube into equal-size regions by brute-force. <br/>
 * That is, consider all divisions on each dimension such that rd[0] * ... * rd[k-1] = r <br/>
 * http://wiki.epfl.ch/bigdata2015-hypercubejoins/hcpartition
 *
 * @author Tam
 * @param <KeyType>
 */
public class CubeNAssignmentBruteForce<KeyType> implements Serializable, HyperCubeAssignment<KeyType> {

	public static long timeout = 1L;

	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(CubeNAssignmentBruteForce.class);

	private Random rand;
	private int[] _rd;
	private final int _r;
	private long[] sizes;
	private Map<String, Integer> regionIDsMap;
	private Comparator<Assignment> _comparator = new CombineCost();

	// Only for testing small join cube, as the cache is equal to the size of join cube (=sizes[0] * ... * sizes[k-1])
	private boolean useRegionMapCache = false;

	public CubeNAssignmentBruteForce(long[] sizes, int r, long randomSeed) {
		rand = randomSeed == -1 ? new Random() : new Random(randomSeed);

		this.sizes = sizes;
		this._rd = new int[sizes.length];
		this._r = r;
		compute();
		if (useRegionMapCache) createRegionMap();
	}

	public CubeNAssignmentBruteForce(long[] sizes, int r, long randomSeed, Comparator<Assignment> comparator) {
		rand = randomSeed == -1 ? new Random() : new Random(randomSeed);

		this.sizes = sizes;
		this._rd = new int[sizes.length];
		this._r = r;
		this._comparator = comparator;
		compute();
		if (useRegionMapCache) createRegionMap();
	}

	private void compute() {
		long compare = _r;
		for (long size : sizes) {
			compare = compare / size;
		}

		// If #joiners larger than the size of join matrix itself, each cell is
		// a partition
		if (compare >= 1) {
			for (int i = 0; i < _rd.length; i++) {
				_rd[i] = (int) sizes[i];
			}
			return;
		}

		// We find the best partition as hypercubes
		int[] rd = new int[_rd.length];

		// Find the prime factors of the _r.
		//final int r = findBestR(_r, 0.5); // Maximum 50% of machines not used
		final int r = _r;
		
		final List<Integer> primeFactors = Utilities.primeFactors(r);

		// Get the Power Set, and iterate over it...
		List<List<Integer>> powerSet = new ArrayList<List<Integer>>(Utilities.powerSet(primeFactors));

		SetArrangementIterator generator = new SetArrangementIterator(powerSet, rd.length);
		int count = 0;
		while (generator.hasNext()) {
			List<List<Integer>> combination = generator.next();
			for (int dim = 0; dim < rd.length; dim++) {
				rd[dim] = Utilities.multiply(combination.get(dim));
			}

			if (Utilities.multiply(rd) != r)
				continue;

			if (count == 0) {
				Utilities.copy(rd, _rd);
			} else {

				// double currentComp = computationCost(sizes, rd);
				// double currentComm = communicationCost(sizes, rd);
				// double bestComp = computationCost(sizes, _rd);
				// double bestComm = communicationCost(sizes, _rd);
				// if (currentComp <= bestComp && currentComm <= bestComm) {
				// Utilities.copy(rd, _rd);
				// }

				// If new assignment is better than the best assignment so far
				if (_comparator.compare(new Assignment(sizes, rd), new Assignment(sizes, _rd)) > 0) {
					Utilities.copy(rd, _rd);
				}
			}
			count++;
		}

	}

	private int findBestR(int r, double tolerate) {
		assert tolerate <= 1 && tolerate >= 0.5;

		int bestR = r;
		List<Integer> bestPrimes = Utilities.primeFactors(r);
		for (int i = r - 1; i > r * (1 - tolerate) && i > 0; i--) {
			List<Integer> primeFactors = Utilities.primeFactors(i);
			if (primeFactors.size() > bestPrimes.size()) {
				bestR = i;
				bestPrimes = primeFactors;
			}
		}
		return bestR;
	}

	private void createRegionMap() {
		regionIDsMap = new HashMap<String, Integer>();
		CellIterator gen = new CellIterator(_rd);
		while (gen.hasNext()) {
			List<Integer> cellIndex = gen.next();
			mapRegionID(cellIndex);
		}
	}

	private int mapRegionID(List<Integer> regionIndex) {
		assert _rd.length == regionIndex.size();

		// Look up at cache first
		if (useRegionMapCache) {
			assert regionIDsMap.containsKey(getMappingIndexes(regionIndex));
			return regionIDsMap.get(getMappingIndexes(regionIndex));
		}

		// Compute if not found in cache
		int regionID = 0;
		for (int i = regionIndex.size() - 1; i >= 0; i--) {
			int dimAmount = regionIndex.get(i);
			for (int dim = _rd.length - 1; dim > i; dim--) {
				dimAmount *= _rd[dim];
			}
			regionID += dimAmount;
		}

		return regionID;
	}

	@Override
	public List<Integer> getRegionIDs(Dimension dim) {
		final List<Integer> regionIDs = new ArrayList<Integer>();

		if (dim.val() >= 0 && dim.val() < sizes.length) {
			final int randomIndex = rand.nextInt(_rd[dim.val()]);
			CellIterator gen = new CellIterator(_rd, dim.val(), randomIndex);
			while (gen.hasNext()) {
				List<Integer> cellIndex = gen.next();
				int regionID = mapRegionID(cellIndex);
				regionIDs.add(regionID);
			}
			assert regionIDs.size() == Utilities.multiply(_rd) / _rd[dim.val()];
		} else {
			LOG.info("ERROR not a possible index assignment.");
		}

		return regionIDs;
	}

	@Override
	public List<Integer> getRegionIDs(Dimension dim, KeyType key) {
		throw new RuntimeException("This method is content-insenstive");
	}

	@Override
	public String toString() {
		return getMappingDimensions();
	}

	@Override
	public String getMappingDimensions() {
		StringBuilder sb = new StringBuilder();
		String prefix = "";
		for (int r : _rd) {
			sb.append(prefix);
			prefix = "-";
			sb.append(r);
		}
		return sb.toString();
	}

	public static String getMappingIndexes(List<Integer> regionIndex) {
		StringBuilder sb = new StringBuilder();
		String prefix = "";
		for (Integer r : regionIndex) {
			sb.append(prefix);
			prefix = "-";
			sb.append(r);
		}
		return sb.toString();
	}

	@Override
	public int getNumberOfRegions(Dimension dim) {
		if (dim.val() >= 0 && dim.val() < _rd.length) {
			return _rd[dim.val()];
		} else {
			throw new RuntimeException("Dimension is invalid");
		}
	}

	/**
	 * The actual number of regions with the best prime factorization.
	 */
	public int getNumberOfRegions() {
		return Utilities.multiply(_rd);
	}

	public static void main(String[] args) {
		List<CubeNAssignmentBruteForce> tests = Arrays.asList(new CubeNAssignmentBruteForce(Utilities.arrayOf(13, 7), 1, -1), new CubeNAssignmentBruteForce(Utilities.arrayOf(4, 4, 4), 8, -1), new CubeNAssignmentBruteForce(Utilities.arrayOf(4, 4, 4, 4), 16, -1), new CubeNAssignmentBruteForce(
				Utilities.arrayOf(8, 4, 10, 7), 1000, -1), new CubeNAssignmentBruteForce(Utilities.arrayOf(10, 10, 10, 10), 1021, -1));
		for (CubeNAssignmentBruteForce test : tests) {
			LOG.info("Input: " + Arrays.toString(test.sizes));
			LOG.info("#Reducers each dimension: " + test.toString());
			for (int i = 0; i < 3; i++) {
				LOG.info("Get Regions of dimension 1: " + test.getRegionIDs(Dimension.d(0)).toString());
			}
		}
	}

}
