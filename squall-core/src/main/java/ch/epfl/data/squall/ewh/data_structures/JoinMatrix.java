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


package ch.epfl.data.squall.ewh.data_structures;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.ujmp.core.Matrix;
import org.ujmp.core.enums.FileFormat;
import org.ujmp.core.matrix.AbstractMatrix;

import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.ewh.visualize.VisualizerInterface;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.utilities.MyUtilities;

// Join Attribute Type
public abstract class JoinMatrix<JAT extends Comparable<JAT>> implements
		SimpleMatrix {
	private static Logger LOG = Logger.getLogger(JoinMatrix.class);

	// matrix could be Boolean, but we will later on add region boundaries in
	// other colors
	protected AbstractMatrix _ujmpMatrix;

	protected List<Region> _regions;
	protected List<JAT> _joinAttributeX = new ArrayList<JAT>();
	protected List<JAT> _joinAttributeY = new ArrayList<JAT>();

	protected ComparisonPredicate _cp; // for finding candidate regions

	private int _totalNumOutputs = -1; // we did not want to add overhead to
										// setElement method;
										// that's why a user needs to set it
										// (EWHSampleMatrixBolt.scaleOutput)

	protected Map<JAT, Integer> _freqX, _freqY; // key, frequency
	protected Map<JAT, Integer> _keyXFirstPos, _keyYFirstPos; // key,
																// firstPosition
	protected NumericConversion _wrapper;

	protected String _matrixName, _matrixPath;

	private void addElement(Map<JAT, Integer> freqList,
			Map<JAT, Integer> keyFirstPos, JAT joinAttribute, int position) {
		if (!freqList.containsKey(joinAttribute)) {
			freqList.put(joinAttribute, 1);
			keyFirstPos.put(joinAttribute, position);
		} else {
			int currentFreq = freqList.get(joinAttribute);
			int newFreq = currentFreq + 1;
			freqList.put(joinAttribute, newFreq);
		}
	}

	public void clearRegions() {
		_regions = null;
	}

	// ***********************************************************************
	// from Matrix interface
	@Override
	public abstract long getCapacity();

	public ComparisonPredicate getComparisonPredicate() {
		return _cp;
	}

	public abstract Map getConfiguration();

	// abstract methods
	public abstract JoinMatrix<JAT> getDeepCopy();

	@Override
	public abstract int getElement(int x, int y);

	public JAT getJoinAttributeX(int position) {
		return _joinAttributeX.get(position);
	}

	public JAT getJoinAttributeY(int position) {
		return _joinAttributeY.get(position);
	}

	public abstract int getMinPositiveValue();

	public Iterator<long[]> getNonEmptyCoordinatesIterator() {
		return _ujmpMatrix.availableCoordinates().iterator();
		/*
		 * Alternatives: _ujmpMatrix.availableCoordinates
		 * _ujmpMatrix.nonZeroCoordinates _ujmpMatrix.allValues
		 */
	}

	// for the sample matrix
	public int getNumCandidatesIterate(Map conf) {
		int result = 0;
		int firstCandInLastLine = 0;
		for (int i = 0; i < getXSize(); i++) {
			boolean isFirstInLine = true;
			int x1 = i;
			int x2 = i;
			for (int j = firstCandInLastLine; j < getYSize(); j++) {
				int y1 = j;
				int y2 = j;
				Region region = new Region(x1, y1, x2, y2);
				boolean isCandidate = MyUtilities.isCandidateRegion(this,
						region, _cp, conf);
				if (isCandidate) {
					result++;
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
		return result;
	}

	@Override
	public long getNumElements() {
		// this will work when reading from a file;
		// it won't work when regions are set, but that does not happen for the
		// invocation of this method
		return _ujmpMatrix.getValueCount();
	}

	public int getNumXElements(JAT key) {
		if (_freqX == null) {
			throw new RuntimeException(
					"Method precomputeFrequencies() must be called before this method (getNumXElements)!");
		} else {
			return _freqX.get(key);
		}
	}

	public int getNumYElements(JAT key) {
		if (_freqY == null) {
			throw new RuntimeException(
					"Method precomputeFrequencies() must be called before this method (getNumYElements)!");
		} else {
			return _freqY.get(key);
		}
	}

	// The cost of WeighPrecomputation is O(n^2)
	// This is cheaper as regions do not cover the entire matrix (large portions
	// of zero-cells are not covered)
	// Cost of this is O(C * m), where C is due to the fact that non all
	// elements within regions are candidate
	// Alternatively (and more efficiently), we could go over all output cells
	// and assign them to the appropriate region (O(m))
	// This is not measured in the algorithm execution time, and thus not
	// important
	public int getRegionNumOutputs(Region region) {
		int numOutputs = 0;
		for (int i = region.get_x1(); i <= region.get_x2(); i++) {
			for (int j = region.get_y1(); j <= region.get_y2(); j++) {
				numOutputs += getElement(i, j);
			}
		}
		return numOutputs;
	}

	public List<Region> getRegions() {
		return _regions;
	}

	public int getTotalNumOutputs() {
		// this will work when reading from a file;
		// it won't work when regions are set, but that does not happen for the
		// invocation of this method
		// old slow version: return (int) _ujmpMatrix.getValueSum();
		return _totalNumOutputs;
		// I could alternatively change the setElement method, but I wanted to
		// avoid the overheads
	}

	public Matrix getUJMPMatrix() {
		return _ujmpMatrix;
	}

	public NumericConversion getWrapper() {
		return _wrapper;
	}

	public int getXFirstKeyPosition(JAT key) {
		if (_keyXFirstPos == null) {
			throw new RuntimeException(
					"Method precomputeFrequencies() must be called before this method (getXFirstKeyPosition)!");
		} else {
			return _keyXFirstPos.get(key);
		}
	}

	// we are always asking for frequency on one on the boundaries of the region
	public FrequencyPosition getXFreqPos(JAT key, Region region) {
		int lowerPos = region.get_x1();
		int upperPos = region.get_x2();

		int freq = 0;
		int smallestKeyPosition = lowerPos; // true when starting from lower
											// border

		// for lower border
		while ((lowerPos <= upperPos)
				&& getJoinAttributeX(lowerPos).equals(key)) {
			freq++;
			lowerPos++;
		}

		// for upper border
		while ((lowerPos <= upperPos)
				&& getJoinAttributeX(upperPos).equals(key)) {
			freq++;
			smallestKeyPosition = upperPos;
			upperPos--;
		}

		return new FrequencyPosition(freq, smallestKeyPosition);
	}

	@Override
	public int getXSize() {
		return (int) _ujmpMatrix.getRowCount();
	}

	public int getYFirstKeyPosition(JAT key) {
		if (_keyYFirstPos == null) {
			throw new RuntimeException(
					"Method precomputeFrequencies() must be called before this method (getYFirstKeyPosition)!");
		} else {
			return _keyYFirstPos.get(key);
		}
	}

	// we are always asking for frequency on one on the boundaries of the region
	public FrequencyPosition getYFreqPos(JAT key, Region region) {
		int lowerPos = region.get_y1();
		int upperPos = region.get_y2();

		int freq = 0;
		int smallestKeyPosition = lowerPos; // true when starting from lower
											// border

		// for lower border
		while ((lowerPos <= upperPos)
				&& getJoinAttributeY(lowerPos).equals(key)) {
			freq++;
			lowerPos++;
		}

		// for upper border
		while ((lowerPos <= upperPos)
				&& getJoinAttributeY(upperPos).equals(key)) {
			freq++;
			smallestKeyPosition = upperPos;
			upperPos--;
		}

		return new FrequencyPosition(freq, smallestKeyPosition);
	}

	@Override
	public int getYSize() {
		return (int) _ujmpMatrix.getColumnCount();
	}

	@Override
	public abstract void increase(int delta, int x, int y);

	// ***********************************************************************

	@Override
	public abstract void increment(int x, int y);

	public abstract boolean isEmpty(int x, int y);

	public void precomputeFrequencies() {
		// for x
		_freqX = new HashMap<JAT, Integer>();
		_keyXFirstPos = new HashMap<JAT, Integer>();
		for (int i = 0; i < getXSize(); i++) {
			addElement(_freqX, _keyXFirstPos, getJoinAttributeX(i), i);
		}
		// System.out.println("FreqX = " + _freqX);

		// for y
		_freqY = new HashMap<JAT, Integer>();
		_keyYFirstPos = new HashMap<JAT, Integer>();
		for (int j = 0; j < getYSize(); j++) {
			addElement(_freqY, _keyYFirstPos, getJoinAttributeY(j), j);
		}
		// System.out.println("FreqY = " + _freqY);
	}

	@Override
	public abstract void setElement(int value, int x, int y);

	public void setJoinAttributeX(JAT key) {
		_joinAttributeX.add(key);
	}

	public void setJoinAttributeY(JAT key) {
		_joinAttributeY.add(key);
	}

	public abstract void setMinPositiveValue(int x, int y);

	public void setRegions(List<Region> regions) {
		_regions = regions;
	}

	public void setTotalNumOutput(int totalNumOutputs) {
		_totalNumOutputs = totalNumOutputs;
	}

	public abstract void visualize(VisualizerInterface visualizer);

	public void writeMatrixToFile() {
		try {
			String path = _matrixPath + "/" + _matrixName;
			_ujmpMatrix.exportToFile(FileFormat.SPARSECSV, new File(path));
		} catch (Exception exc) {
			LOG.info(MyUtilities.getStackTrace(exc));
		}
		// PLT format is the only thing we could use for saving graphs for the
		// papers
	}
}