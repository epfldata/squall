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

public class MatrixIntInt implements SimpleMatrix {

    private int[][] _matrix;
    private int _xSize, _ySize;

    @Override
    public long getCapacity() {
	return ((long) _xSize) * _ySize;
    }

    @Override
    public long getNumElements() {
	return ((long) _xSize) * _ySize;
    }

    @Override
    public int getXSize() {
	return _xSize;
    }

    @Override
    public int getYSize() {
	return _ySize;
    }

    public MatrixIntInt(int xSize, int ySize) {
	_xSize = xSize;
	_ySize = ySize;
	_matrix = new int[_xSize][_ySize];
    }

    @Override
    public int getElement(int x, int y) {
	return _matrix[x][y];
    }

    @Override
    public void setElement(int value, int x, int y) {
	_matrix[x][y] = value;
    }

    @Override
    public void increase(int delta, int x, int y) {
	_matrix[x][y] += delta;
    }

    @Override
    public void increment(int x, int y) {
	increase(1, x, y);
    }
}