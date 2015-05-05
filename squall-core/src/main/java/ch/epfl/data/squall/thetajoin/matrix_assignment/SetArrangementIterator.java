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
 * The idea is the same as ArrangementIterator.java.
 * But how instead of arranging the numbers, you arrange the sets (list of sets), each set is a list of integer.
 *
 * @author Tam
 */
public class SetArrangementIterator implements Iterator<List<List<Integer>>> {

    private final List<List<Integer>> _sets;
    private final int _n;
    private int[] _pos;
    private boolean _hasNext = true;

    public SetArrangementIterator(List<List<Integer>> sets, int length){
        _sets = sets;
        _n = sets.size();
        _pos = new int[length];
        Arrays.fill(_pos, 0);
    }

    @Override
    public boolean hasNext() {
        return _hasNext;
    }

    @Override
    public List<List<Integer>> next() {
        List<List<Integer>> res = new ArrayList<List<Integer>>();
        for (int p : _pos){
            res.add(_sets.get(p));
        }

        for (int i = 0; i < _pos.length; i++){
            if (_pos[i] == _n - 1){
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
        List<List<Integer>> powerSet = new ArrayList<List<Integer>>(Utilities.powerSet(Utilities.primeFactors(8)));
        SetArrangementIterator me = new SetArrangementIterator(powerSet, 3);
        int count = 0;
        while (me.hasNext()){
            count++;
            List<List<Integer>> combination = me.next();
            System.out.println(combination.toString());
        }
        assert count == powerSet.size() * powerSet.size() * powerSet.size();
    }

}
