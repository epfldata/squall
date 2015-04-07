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


package ch.epfl.data.squall.thetajoin.matrix_assignment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.ewh.data_structures.KeyRegion;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.DeepCopy;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

/**
 * @author ElSeidy This class performs content sensitive region assignments to
 *         Matrix
 */
public class ContentSensitiveMatrixAssignment<KeyType extends Comparable<KeyType>>
		implements Serializable, MatrixAssignment<KeyType> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger
			.getLogger(ContentSensitiveMatrixAssignment.class);
	private long _sizeS, _sizeT; // dimensions of data.. row, column
									// respectively.
	private int _r; // practically speaking usually a relatively small value!
	private int[][] regionsMatrix;
	private List<KeyRegion> keyRegions = null;
	private Random rnd = new Random();
	private Type<KeyType> _wrapper;

	public ContentSensitiveMatrixAssignment(Map map) {
		String shortName = SystemParameters.getString(map,
				"PARTITIONING_ALGORITHM");
		int numJoiners = SystemParameters.getInt(map, "PAR_LAST_JOINERS");
		int numBuckets = SystemParameters.getInt(map, "FIRST_NUM_OF_BUCKETS");
		String keyRegionFilename = MyUtilities.getKeyRegionFilename(map,
				shortName, numJoiners, numBuckets);
		LOG.info("Reading keyRegions from " + keyRegionFilename);
		keyRegions = (List<KeyRegion>) DeepCopy
				.deserializeFromFile(keyRegionFilename);
	}

	@Override
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn) {
		throw new RuntimeException(
				"This method is contentsenstive needs tuple key");
	}

	@Override
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn, KeyType key) {

		double rndValue = rnd.nextDouble();
		final ArrayList<Integer> candidateRegions = new ArrayList<Integer>();
		if (RowOrColumn == Dimension.ROW) {
			// Then we are exploring the x-dimension
			for (Iterator<KeyRegion> iterator = keyRegions.iterator(); iterator
					.hasNext();) {
				KeyRegion kRG = iterator.next();
				if (kRG.get_kx1().compareTo(key) <= 0
						&& kRG.get_kx2().compareTo(key) >= 0) {
					if (kRG.get_kx1().compareTo(key) != 0
							&& kRG.get_kx2().compareTo(key) != 0) {
						candidateRegions.add(kRG.getRegionIndex());
					} else if (kRG.get_kx1().compareTo(key) == 0) {
						if (kRG.get_kx1ProbLowerPos() <= rndValue
								&& kRG.get_kx1ProbUpperPos() > rndValue) {
							candidateRegions.add(kRG.getRegionIndex());
						}
					} else if (kRG.get_kx2().compareTo(key) == 0) {
						if (kRG.get_kx2ProbLowerPos() <= rndValue
								&& kRG.get_kx2ProbUpperPos() > rndValue) {
							candidateRegions.add(kRG.getRegionIndex());
						}
					}
				}
			}
		} else {
			// Else we are exploring the y-dimension
			for (Iterator<KeyRegion> iterator = keyRegions.iterator(); iterator
					.hasNext();) {
				KeyRegion kRG = iterator.next();
				if (kRG.get_ky1().compareTo(key) <= 0
						&& kRG.get_ky2().compareTo(key) >= 0) {
					if (kRG.get_ky1().compareTo(key) != 0
							&& kRG.get_ky2().compareTo(key) != 0) {
						candidateRegions.add(kRG.getRegionIndex());
					} else if (kRG.get_ky1().compareTo(key) == 0) {
						if (kRG.get_ky1ProbLowerPos() <= rndValue
								&& kRG.get_ky1ProbUpperPos() > rndValue) {
							candidateRegions.add(kRG.getRegionIndex());
						}
					} else if (kRG.get_ky2().compareTo(key) == 0) {
						if (kRG.get_ky2ProbLowerPos() <= rndValue
								&& kRG.get_ky2ProbUpperPos() > rndValue) {
							candidateRegions.add(kRG.getRegionIndex());
						}
					}
				}
			}
		}
		/*
		 * System.out.print(candidateRegions.size()+":"); for (int i = 0; i <
		 * candidateRegions.size(); i++) {
		 * System.out.print(candidateRegions.get(i)+","); }
		 * System.out.println();
		 */
		// if(candidateRegions.size()==0)
		// System.out.println("key is "+key+" from relation "+ RowOrColumn);

		if (candidateRegions.isEmpty()) {
			throw new RuntimeException(
					"It cannot be that a tuple is sent nowhere! For tuple with key "
							+ key);
		}

		return candidateRegions;
	}

	@Override
	public String toString() {
		return keyRegions.toString();
	}

}