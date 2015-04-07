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

import org.ujmp.core.intmatrix.impl.DefaultSparseIntMatrix;

public class SparseMatrixUJMP implements SimpleMatrix {

	private DefaultSparseIntMatrix _ujmpMatrix;
	private int _xSize, _ySize;
	private int _capacity; // max allowed num of elements

	public SparseMatrixUJMP(int capacity, int xSize, int ySize) {
		_xSize = xSize;
		_ySize = ySize;
		_capacity = capacity;
		_ujmpMatrix = new DefaultSparseIntMatrix(_capacity, new long[] {
				_xSize, _ySize }); // The first argument is MAX_SIZE
	}

	@Override
	public long getCapacity() {
		return _capacity;
	}

	@Override
	public int getElement(int x, int y) {
		return _ujmpMatrix.getAsInt(x, y);
	}

	@Override
	public long getNumElements() {
		return _ujmpMatrix.getValueCount();
	}

	/*
	 * if needed, we could use the following as well public SparseMatrixUJMP(int
	 * xSize, int ySize) { _xSize = xSize; _ySize = ySize; //int capacity = 2 *
	 * (_xSize + _ySize); // the number of candidates is slightly more than
	 * 2*xSize; to be on the safe side, we choose 2 * (xSize + ySize) int
	 * capacity = SystemParameters.MATRIX_CAPACITY_MULTIPLIER * (xSize + ySize);
	 * _ujmpMatrix = new DefaultSparseIntMatrix(capacity, new long[]{_xSize,
	 * _ySize}); // The first argument is MAX_SIZE }
	 */

	@Override
	public int getXSize() {
		return _xSize;
	}

	@Override
	public int getYSize() {
		return _ySize;
	}

	@Override
	public void increase(int delta, int x, int y) {
		int oldValue = getElement(x, y);
		int newValue = oldValue + delta;
		setElement(newValue, x, y);
	}

	@Override
	public void increment(int x, int y) {
		increase(1, x, y);
	}

	@Override
	public void setElement(int value, int x, int y) {
		_ujmpMatrix.setAsInt(value, x, y);
	}

}
