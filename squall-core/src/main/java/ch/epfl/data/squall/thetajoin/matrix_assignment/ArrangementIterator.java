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
 * This class helps you to iterate over all arrangements of n factors into m positions.
 * For example, given two factors {2,5} and 2 positions, you will have iterations: (2,2), (2,5), (5,2), (5,5)
 * 
 * @author Tam
 */
public class ArrangementIterator implements Iterator<List<Integer>>{
	
	private final List<Integer> _factors;
	private final int _nFactors;
	private int[] _pos;
	private boolean _hasNext = true;
	private boolean _noDuplicate = false; // if true it becomes permutation
	
	public ArrangementIterator(List<Integer> factors, int length){
		_factors = factors;
		_nFactors = factors.size();
		_pos = new int[length];
		Arrays.fill(_pos, 0);
	}
	
	/**
	 * @param noDuplicate if set to true, it becomes permutation. 
	 * For example, given two factors {2,5} and 2 positions, you will have iterations: (2,5), (5,2)
	 */
	public ArrangementIterator(List<Integer> factors, int length, boolean noDuplicate){
		_factors = factors;
		_nFactors = factors.size();
		_pos = new int[length];
		Arrays.fill(_pos, 0);
		_noDuplicate = noDuplicate;
	}

	@Override
	public boolean hasNext() {
		return _hasNext;
	}

	@Override
	public List<Integer> next() {
		List<Integer> res = new ArrayList<Integer>();
		for (int p : _pos){
			res.add(_factors.get(p));
		}
		
		for (int i = 0; i < _pos.length; i++){
			if (_pos[i] == _nFactors - 1){
				if (i == _pos.length - 1) _hasNext = false;
				_pos[i] = 0;
			} else {
				_pos[i]++;
				if (_noDuplicate) for (int j = 0; j < i; j++) _pos[j] = _pos[i];
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
		ArrangementIterator me = new ArrangementIterator(Arrays.asList(3,4,5), 3);
		int count = 0;
		while (me.hasNext()){
			count++;
			List<Integer> combination = me.next();
			System.out.println(combination.toString());
		}
		assert count == 27;
	}

}
