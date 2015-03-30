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

import java.io.Serializable;

/**
 * @author Zervos Theta-Join Index interface. All indexes (Hash, B+, etc) should
 *         implement this). Key is a string and value is a list of row-ids in
 *         the storage structures (where the tuples are saved)
 */
public interface Index<KeyType> extends Serializable {

	public TIntArrayList getValues(int operator, KeyType key);

	public TIntArrayList getValuesWithOutOperator(KeyType key, KeyType... keys);

	public void put(Integer row_id, KeyType key);

	public void remove(Integer row_id, KeyType key);
}