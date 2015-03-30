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


package ch.epfl.data.squall.storage;

import java.util.List;

/*
 * Key, Tuple(s)
 */
public interface BPlusTreeStorage<KeyType> {

	/**
	 * Give it the operator type as defined in "ComparisonPredicate class" e.g.
	 * ComparisonPredicate.EQUAL_OP, and give it key of type "KeyType" and
	 * returns a list of tuples of type "String" ~ This interface can be changed
	 * if required for any reasons
	 * 
	 * @param operator
	 * @param key
	 * @return
	 */
	// TODO implement all operations for operator --> FOR EXAMPLE SEE public
	// "TIntArrayList getValues(int operator, KeyType key)" in BPLUSTREEINDEX
	// class
	public List<String> get(int operator, KeyType key, int diff);

	public String getStatistics();

	/**
	 * Purge stale state
	 * 
	 * @return
	 */
	public void purgeState(long tillTimeStamp);

	/**
	 * Give it key of type KeyType and string value and putinto BerkeleyDB
	 * 
	 * @param key
	 * @param value
	 */
	public void put(KeyType key, String value);

	public void shutdown();

	/**
	 * Return the size of the storage (the number of tuples stored inside)
	 * 
	 * @return
	 */
	public int size();

}
