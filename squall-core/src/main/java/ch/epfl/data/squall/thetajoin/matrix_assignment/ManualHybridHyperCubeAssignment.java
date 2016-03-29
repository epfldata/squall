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

public class ManualHybridHyperCubeAssignment implements Serializable, HybridHyperCubeAssignment {

	private Random rand;
    private static Logger LOG = Logger.getLogger(ManualHybridHyperCubeAssignment.class);

	Map<String, Dimension> dimensions;
	int[] dimensionSizes; 

	Map<String, Integer> regionIDsMap;

	public ManualHybridHyperCubeAssignment(Map<String, Dimension> dimensions) {
		rand = new Random();
		this.dimensions = dimensions;

		createDimensionSizes();
		createRegionMap();
	}

	public void createDimensionSizes() {
		dimensionSizes = new int[dimensions.size()];

		for (String key : dimensions.keySet()) {
			Dimension d = dimensions.get(key);
			dimensionSizes[d.index] = d.size;
		}
	}

	@Override
	public List<Integer> getRegionIDs(String emitterName, Map<String, String> c) {
		if (isRandom(emitterName)) {
			return getRandomRegion(emitterName);
		} else {
			return getHashRegion(c);
		}
	}

	public List<Integer> getRandomRegion(String emitterName) {
		final List<Integer> regionIDs = new ArrayList<Integer>();
		
		Dimension dim = dimensions.get(emitterName);
		final int fixedIndex = rand.nextInt(dim.size);
		
		CellIterator gen = new CellIterator(dimensionSizes, dim.index, fixedIndex);

		while (gen.hasNext()) {
			List<Integer> cellIndex = gen.next();
			int regionID = mapRegionID(mapRegionKey(cellIndex));
			regionIDs.add(regionID);
		}

		return regionIDs;
	}

	public List<Integer> getHashRegion(Map<String, String> c) {
		List<Integer> regions = new ArrayList<Integer>();
		List<Integer> fixedDim = new ArrayList<Integer>();
		List<Integer> fixedIndex = new ArrayList<Integer>();

		for (String columnName : c.keySet()) {
			if (dimensions.containsKey(columnName)) {
				Dimension dim = dimensions.get(columnName);

				String value = c.get(columnName);
				int hashValue = Math.abs(value.hashCode()) % dim.size;
				
				fixedDim.add(dim.index);
				fixedIndex.add(hashValue);
			}
		}

		CellIterator gen = new CellIterator(dimensionSizes, toIntArray(fixedDim), toIntArray(fixedIndex));
		while (gen.hasNext()) {
			List<Integer> cellIndex = gen.next();
			int regionID = mapRegionID(mapRegionKey(cellIndex));
			regions.add(regionID);
		}

		return regions;
	}

	public int[] toIntArray(List<Integer> list) {
		int[] tmp = new int[list.size()];
		int i = 0;

		for (Integer number : list) {
			tmp[i++] = number;
		}

		return tmp;
	}

	private boolean isRandom(String emitterName) {
		return dimensions.containsKey(emitterName);
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
		for (String key : dimensions.keySet()) {
			sb.append(dimensions.get(key).name).append(" : ").append(dimensions.get(key).size);
			sb.append("\n");
		}
		return sb.toString();
	}

	public static class Dimension implements Serializable {
		public String name;
		public int size;
		public int index;

		public Dimension(String name, int size, int index) {
			this.name = name;
			this.size = size;
			this.index = index;
		}

		public Dimension(String name, int index) {
			this.name = name;
			this.index = index;
		}

		public String toString() {
			return name + ", " + size + ", " + index;
		}
	}
}