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

import ch.epfl.data.squall.storm_components.hash_hypercube.HashHyperCubeGrouping.EmitterDesc;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ManualHybridHyperCubeAssignment.Dimension;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HashHyperCubeAssignmentBruteForce.ColumnDesc;

import ch.epfl.data.squall.types.Type;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Random;

public class HybridHyperCubeAssignmentBruteForce implements Serializable, HybridHyperCubeAssignment {

	private Random rand;
    private static Logger LOG = Logger.getLogger(HybridHyperCubeAssignmentBruteForce.class);

	List<EmitterDesc> emitters;
	List<ColumnDesc> columns;
	Set<String> randomColumns;
	int[] dimensionSizes; 
	int reducers;
	Map<String, Integer> regionIDsMap;

	public HybridHyperCubeAssignmentBruteForce(List<EmitterDesc> emitters, List<ColumnDesc> columns, 
		Set<String> randomColumns, int reducers) {
		
		rand = new Random();

		this.emitters = emitters;
		this.columns = columns;
		this.randomColumns = randomColumns;
		this.reducers = reducers;

		compute();
		createDimensionSizes();
		createRegionMap();
	}

	private void compute() {
		for (int i = 1; i <= reducers; i++) {
			int[] best = compute(i);

			if (dimensionSizes == null) {
				dimensionSizes = new int[best.length];
				Utilities.copy(best, dimensionSizes);
			}

			// If new assignment is better than the best assignment so far
			if (compareWorkloads(best, dimensionSizes) < 0) {
				Utilities.copy(best, dimensionSizes);
			}
		}
	}

	private int[] compute(int r) {
		LOG.info("Calulating for : " + r);
		int[] partition = new int[columns.size()];
		
		// Find the prime factors of the r.
		final List<Integer> primeFactors = Utilities.primeFactors(r);

		// Get the Power Set, and iterate over it...
		List<List<Integer>> powerSet = new ArrayList<List<Integer>>(Utilities.powerSet(primeFactors));

		SetArrangementIterator generator = new SetArrangementIterator(powerSet, partition.length);
		int count = 0;
		int[] rd = new int[columns.size()];
		
		while (generator.hasNext()) {
			List<List<Integer>> combination = generator.next();
			for (int dim = 0; dim < rd.length; dim++) {
				rd[dim] = Utilities.multiply(combination.get(dim));
			}

			if (Utilities.multiply(rd) != r)
				continue;


			String res = "";
			for (int i : rd) res += " " + i;
			LOG.info("For : " + r + " Found : " + res);
			LOG.info("Workload is  : " + getWorkload(rd));

			if (count == 0) {
				Utilities.copy(rd, partition);
			} else {
				// If new assignment is better than the best assignment so far
				if (compareWorkloads(rd, partition) < 0) {
					Utilities.copy(rd, partition);
				}
			}
			count++;
		}

		return partition;
	}

	int compareWorkloads(int[] p1, int[] p2) {
		long workload1 = getWorkload(p1);
		long workload2 = getWorkload(p2);

		int maxDim1 = getMaxDimension(p1);
		int maxDim2 = getMaxDimension(p2);

		if (workload1 < workload2)
			return -1;
		else if (workload2 < workload1)
			return 1;
		else // choose wich has lower maximum dimension size
			return maxDim1 - maxDim2;
	}
	
	public long getWorkload(int[] partition) {
		long workload = 0;
		for (EmitterDesc emitter : emitters) {

			Set<String> emitterColumns = new HashSet<String>(Arrays.asList(emitter.columnNames));

			int replicate = 1;
			for (int i = 0; i < partition.length; i++) {
				if (emitterColumns.contains(columns.get(i).name)) {
					replicate *= partition[i];					
				}
			}

			workload += emitter.cardinality / replicate;
		}

		return workload;
	}

	public int getMaxDimension(int[] partition) {
		int max = partition[0];

		for (int i = 0; i < partition.length; i++) {
			max = Math.max(max, partition[i]);
		}

		return max;
	}

	public void createDimensionSizes() {
		regionIDsMap = new HashMap<String, Integer>();
		CellIterator gen = new CellIterator(dimensionSizes);
		int i = 0;
		while (gen.hasNext()) {
			List<Integer> cellIndex = gen.next();
			regionIDsMap.put(mapRegionKey(cellIndex), i++);
		}
	}

	@Override
	public List<Integer> getRegionIDs(String emitterName, Map<String, String> c) {
		List<Integer> regions = new ArrayList<Integer>();
		int[] fixedDim = new int[c.size()];
		int[] fixedIndex = new int[c.size()];
		int index = 0;

		for (int i = 0; i < columns.size(); i++) {
			if (c.containsKey(columns.get(i).name)) {
				// calculate hash value
				String value = c.get(columns.get(i).name);

				int hashValue = Math.abs(value.hashCode()) % dimensionSizes[i];
				// if random 
				if (randomColumns.contains(columns.get(i).name)) {
					hashValue = rand.nextInt(dimensionSizes[i]);
				}

				fixedDim[index] = i;
				fixedIndex[index] = hashValue;
				index++;
			}
		}

		CellIterator gen = new CellIterator(dimensionSizes, fixedDim, fixedIndex);
			
		while (gen.hasNext()) {
			List<Integer> cellIndex = gen.next();
		
			int regionID = regionIDsMap.get(mapRegionKey(cellIndex));

			regions.add(regionID);
		}
		return regions;
	}

	private void createRegionMap() {
		regionIDsMap = new HashMap<String, Integer>();
		CellIterator gen = new CellIterator(dimensionSizes);
		int i = 0;
		while (gen.hasNext()) {
			List<Integer> cellIndex = gen.next();
			regionIDsMap.put(mapRegionKey(cellIndex), i++);
		}
	}

	private int mapRegionID(String key) {
		return regionIDsMap.get(key);
	}

	private String mapRegionKey(List<Integer> cellIndex) {
		StringBuilder key = new StringBuilder("");
		for (Integer index : cellIndex) {
			key.append(" " + index);
		}

		return key.toString();
	}

	@Override
	public int getNumberOfRegions(String column) {
		throw new RuntimeException("Dimension is invalid");
	}

	@Override
	public String getMappingDimensions() {
		StringBuilder sb = new StringBuilder();
		String prefix = "";
		for (int i = 0; i < dimensionSizes.length; i++) {
			sb.append(prefix);
			prefix = "-";
			sb.append("[ ").append(columns.get(i).name).append(" : ").append(dimensionSizes[i]).append(" ]");
		}
		return sb.toString();
	}
}