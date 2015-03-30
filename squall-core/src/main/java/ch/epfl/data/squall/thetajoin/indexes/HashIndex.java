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


package ch.epfl.data.squall.thetajoin.indexes;

import gnu.trove.list.array.TIntArrayList;

import java.util.HashMap;

import ch.epfl.data.squall.predicates.ComparisonPredicate;

/**
 * @author Zervos The Theta-Join Hash index used for equalities. Uses a string
 *         as a key and holds a list of row-id's of TupleStorage that have the
 *         actual tuples that correspond to the key.
 */
public class HashIndex<KeyType> implements Index<KeyType> {

	private static final long serialVersionUID = 1L;

	private final HashMap<KeyType, TIntArrayList> _index;

	public HashIndex() {
		_index = new HashMap<KeyType, TIntArrayList>();
	}

	@Override
	public TIntArrayList getValues(int operator, KeyType key) {
		if (operator != ComparisonPredicate.EQUAL_OP)
			return null;
		else
			return getValuesWithOutOperator(key);
	}

	@Override
	public TIntArrayList getValuesWithOutOperator(KeyType key, KeyType... keys) {
		return _index.get(key);
	}

	@Override
	public void put(Integer row_id, KeyType key) {
		TIntArrayList idsList = _index.get(key);
		if (idsList == null) {
			idsList = new TIntArrayList(1);
			_index.put(key, idsList);
		}
		idsList.add(row_id);

	}

	@Override
	public void remove(Integer row_id, KeyType key) {
		TIntArrayList idsList = _index.get(key);
		if (idsList == null)
			throw new RuntimeException(
					"Error: Removing a nonexisting key from index");
		idsList.remove(row_id);
		// int removeIndex=idsList.indexOf(row_id);
		// idsList.remove(removeIndex);
	}

}
