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

import gnu.trove.list.array.TIntArrayList;

import java.io.Serializable;

// T has to be Integer
public class ListTIntAdapter<T extends Comparable<T>> implements
	ListAdapter<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private TIntArrayList _tList = new TIntArrayList();

    @Override
    public void set(int index, T t) {
	_tList.set(index, (Integer) t);
    }

    // the invocation has a non-primitive type T as an argument
    @Override
    public void add(T t) {
	_tList.add((Integer) t);
    }

    // the invocation expects T
    @Override
    public T get(int index) {
	return (T) (Integer) _tList.get(index);
    }

    @Override
    public void remove(int index) {
	_tList.remove(index);
    }

    // works nicely if we spent most of the time in this method
    @Override
    public void sort() {
	_tList.sort();
    }

    @Override
    public int size() {
	return _tList.size();
    }

    @Override
    public String toString() {
	return _tList.toString();
    }
}