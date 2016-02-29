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
import ch.epfl.data.squall.types.Type;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

public class HashHyperCubeAssignmentBruteForce implements Serializable, HashHyperCubeAssignment {

	int reducers;
	List<EmitterDesc> emitters;
	List<ColumnDesc> columns;
	int[] dimensions;

	public HashHyperCubeAssignmentBruteForce(int reducers, List<ColumnDesc> columns, List<EmitterDesc> emitters) {
		this.reducers = reducers;
		this.columns = columns;
		this.emitters = emitters;
		compute();
	}

	private void compute() {
		for (int i = columns.size(); i <= reducers; i++) {
			int[] best = compute(i);
			
			// If new assignment is better than the best assignment so far
			if (compareWorkloads(best, dimensions) < 0) {
				Utilities.copy(best, dimensions);
			}
		}
	}

	private int[] compute(int r) {
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
		else
			return maxDim1 - maxDim2;
	}

	public long getWorkload(int[] partition) {
		long workload = 0;

		for (EmitterDesc emitter : emitters) {
			Set<String> emitterColumns = new HashSet<String>(Arrays.asList(emitter.columnNames));
			int replicate = 1;
			for (int i = 0; i < partition.length; i++) {
				if (!emitterColumns.contains(columns.get(i).name)) {
					replicate *= partition[i];					
				}
			}

			workload = replicate * emitter.cardinality;
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

	@Override
	public List<Integer> getRegionIDs(Map<String, Object> columns) {
		// ????
		return null;
	}

	@Override
	public int getNumberOfRegions(String column) {
		for (ColumnDesc c : columns) {
			if (c.name == column) {
				return c.dimension;
			}
		}
		throw new RuntimeException("Dimension is invalid");
	}

	@Override
	public String getMappingDimensions() {
		StringBuilder sb = new StringBuilder();
		for (ColumnDesc c : columns) {
			sb.append(c.name + " : " + c.dimension + "\n");
		}

		return sb.toString();
	}


	public static class ColumnDesc {
		public String name;
		public Type type;

		public int dimension;
		public long size;

		public ColumnDesc(String name, Type type, long size) {
			this(name, type, size, -1);
		}

		public ColumnDesc(String name, Type type, long size, int dimension) {
			this.name = name;
			this.type = type;
			this.size = size;
			this.dimension = dimension;
		}
	}
}