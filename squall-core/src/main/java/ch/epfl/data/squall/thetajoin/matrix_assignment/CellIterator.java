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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * This iterator helps to iterate over the 'cell' index of a hypercube. 
 * For example, if we have a matrix 2 x 2, it will iterate over (0,0), (0,1), (1,0), (1,1).
 * 
 * @author Tam
 */
public class CellIterator implements Iterator<List<Integer>>{
	
	private final int[] _limits;
	private final int[] _pivots;
	private int[] _pos;
	private boolean _hasNext = true;
	
	/**
	 * This helps you to iterate over all the cells of a hypercube.
	 * @param limits the size of each dimension of a hypercube
	 */
	public CellIterator(int[] limits){
		this(limits, -1, 0);
	}
	
	/**
	 * This helps you to iterative over the cells of a hypercube while fixing a dimension.
	 * For example, given a 3 x 3 matrix, fixing the first dimension with index = 1 gives you the iteration of (1,0), (1,1), (1,2).
	 * @param limits the size of each dimension of a hypercube
	 * @param fixedDim the dimension you want to fix
	 * @param fixedIndex the index of the fixed dimension
	 */
	public CellIterator(int[] limits, int fixedDim, int fixedIndex){
		this(limits, new int[]{fixedDim}, new int[]{fixedIndex});
	}

	public CellIterator(int[] limits, int[] fixedDim, int[] fixedIndex){
		_limits = limits;
		_pos = new int[limits.length];
		Arrays.fill(_pos, 0);
		_pivots = fixedDim;
		
		for (int i = 0; i < fixedDim.length; i++) {
			if (_pivots[i] >= 0 && _pivots[i] < _pos.length) {
				_pos[_pivots[i]] = fixedIndex[i];
			}
		}
	}

	@Override
	public boolean hasNext() {
		return _hasNext;
	}

	@Override
	public List<Integer> next() {
		List<Integer> res = new ArrayList<Integer>();
		for (int p : _pos){
			res.add(p);
		}
		
		for (int i = 0; i < _pos.length; i++){
			boolean fixed = false;
			for (int j = 0; j < _pivots.length; j++) {
				if (i == _pivots[j])  {
					if (i == _pos.length - 1) _hasNext = false;
					fixed = true;
					break;
				}
			}

			if (fixed) continue;
			
			if (_pos[i] == _limits[i] - 1){
				if (i == _pos.length - 1) _hasNext = false;
				_pos[i] = 0;
			} else {
				_pos[i]++;
				break;
			}
		}
		
		return res;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	public static void main(String... args){
		testcase1();
		testcase2();
		testcase3();
		testcase4();
		testcase5();
	}
	
	public static void testcase1(){
		int[] rd = {2,5,2};
		CellIterator me = new CellIterator(rd);
		int count = 0;
		while (me.hasNext()){
			count++;
			List<Integer> combination = me.next();
			System.out.println(combination.toString());
		}
		assert count == Utilities.multiply(rd);
	}
	
	public static void testcase2(){
		int[] rd = {2,5,2};
		CellIterator me = new CellIterator(rd, 2, 3);
		int count = 0;
		while (me.hasNext()){
			count++;
			List<Integer> combination = me.next();
			System.out.println(combination.toString());
		}
		assert count == 10;
	}
	
	public static void testcase3(){
		int[] rd = {5,4,10,5};
		CellIterator me = new CellIterator(rd);
		int count = 0;
		while (me.hasNext()){
			count++;
			List<Integer> combination = me.next();
			System.out.println(combination.toString());
		}
		assert count == Utilities.multiply(rd);
	}
	
	public static void testcase4(){
		int[] rd = {5,4,10,5};
		CellIterator me = new CellIterator(rd, 2, 7);
		int count = 0;
		while (me.hasNext()){
			count++;
			List<Integer> combination = me.next();
			System.out.println(combination.toString());
		}
		assert count == 100;
	}

	public static void testcase5(){
		int[] rd = {2,5,2};
		CellIterator me = new CellIterator(rd, new int[]{0, 2}, new int[]{1, 0});
		int count = 0;
		while (me.hasNext()){
			count++;
			List<Integer> combination = me.next();
			System.out.println(combination.toString());
		}
		assert count == 5;
	}

}
