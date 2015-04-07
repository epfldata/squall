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

/*
 * A replacement algorithm is rensponsible for deciding which element of the
 * in-memory data store to evict when necessary. Every such algorithm should
 * basically provide functionalities for adding and removing objects with the
 * algorithm, methods for accessing those objects as well as a debugging method.
 * When instantiated, value V defines the type of objects this replacement
 * algorithm will handle (use Object if you use objects of different types).
 */
public interface ReplacementAlgorithm<V> {

	/*
	 * A replacement algorithm may need to encapsulate the object of a in memory
	 * store inside another object (e.g. see LRU). Given this, the semantics of
	 * add is as follows: it is given an object V as argument, which registers
	 * obj with the algorithm. If the algorithm has encapsulated obj inside a
	 * larger object, then add returns this container object. The stores must
	 * store this object instead of the internal one for future references (if
	 * needed)
	 */
	public Object add(V obj);

	/*
	 * Function that un-encapsulates the given replacement algorithm object,
	 * which should be an object returned by a call to add
	 */
	public V get(Object obj);

	/*
	 * Stores may want just to read the element that will be removed by the next
	 * call to remove(), without actually removing the element from the
	 * replacement algorithm. This is the functionality of getLast(), which
	 * un-encapsualates the element to be removed by the next remove call, and
	 * returns it
	 */
	public V getLast();

	/*
	 * Print the list of objects, as specified by the replacement algorithm.
	 * Useful for debugging
	 */
	public void print();

	/*
	 * Removes and returns the least prefered element of the data structure. If
	 * the object was encapsulated inside a larger object, then the object is
	 * un-encapsulated and the internal object obj V is returned.
	 */
	public V remove();

	/*
	 * Reset (clear) the replacement algorithm information. This function
	 * doesn't necessarily have to perform cleanup of the objects of the stores.
	 */
	public void reset();
}
