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


package ch.epfl.data.squall.utilities.thetajoin.dynamic;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.tuple.Tuple;

public class ThetaJoinUtilities {
	public static boolean assertDimensions(String inputDim, String originalDim,
			Tuple tuple) // 1
	// normal
	// 2
	// datamigration
	{
		if (inputDim.equals(originalDim))
			return true;
		else
			LOG.info("Error in dimensions inputDim:" + inputDim
					+ " originalDim:" + originalDim + " Tuple: " + tuple);
		return false;
	}

	public static long bytesToMegabytes(long bytes) {
		return bytes / MEGABYTE;
	}

	/**
	 * Checks if joinParams contain value joinIndex
	 * 
	 * @param joinParams
	 * @param joinIndex
	 * @return
	 */
	public static boolean contains(List<Integer> joinParams, int joinIndex) {
		boolean exists = false;
		for (int i = 0; i < joinParams.size(); i++)
			if (joinParams.get(i) == joinIndex) {
				exists = true;
				break;
			}
		return exists;
	}

	/**
	 * Creates the output tuple of the theta join. Contains all fields of
	 * relation 1 in the original order followed by all the fields of relation 2
	 * in the original order except the join keys.
	 * 
	 * @param firstTuple
	 *            The first tuple
	 * @param secondTuple
	 *            The second tuple
	 * @param joinIndicesB
	 *            Join key indices of relation 2
	 * @return The output tuple
	 */
	public static List<String> createThetaOutputTuple(List<String> firstTuple,
			List<String> secondTuple, List<Integer> equiJoinOmitRelBIndices) {
		final List<String> outputTuple = new ArrayList<String>();

		for (int j = 0; j < firstTuple.size(); j++)
			// first relation (R)
			outputTuple.add(firstTuple.get(j));
		for (int j = 0; j < secondTuple.size(); j++)
			if (!ThetaJoinUtilities.contains(equiJoinOmitRelBIndices, j))
				// does
				// not
				// exits
				// add
				// the
				// column!!
				// (S)
				outputTuple.add(secondTuple.get(j));
		return outputTuple;
	}

	public static int[] getDimensions(String Dim) {
		final String[] dimString = Dim.split("-");
		return new int[] { Integer.parseInt(new String(dimString[0])),
				Integer.parseInt(new String(dimString[1])) };

	}

	public static List<String> getJoinKeyValues(List<String> tuple,
			List<Integer> joinKeyIndices) {
		final ArrayList<String> values = new ArrayList<String>();
		for (final int ind : joinKeyIndices) {
			final String val = tuple.get(ind);
			values.add(val);
		}
		return values;
	}

	public static void printMemory() {
		// Get the Java runtime
		final Runtime runtime = Runtime.getRuntime();
		// Run the garbage collector
		runtime.gc();
		// Calculate the used memory
		final long memory = runtime.totalMemory() - runtime.freeMemory();
		LOG.info("Used memory is bytes: " + memory);
		LOG.info("Used memory is megabytes: " + bytesToMegabytes(memory));
	}

	private static Logger LOG = Logger.getLogger(ThetaJoinUtilities.class);

	private static final long MEGABYTE = 1024L * 1024L;

}
