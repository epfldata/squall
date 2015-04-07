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

import java.util.Comparator;
import java.util.PriorityQueue;

public class FixedSizePriorityQueue<T> extends PriorityQueue<T> {
	private final int _maxSize;

	/**
	 * Constructs a {@link FixedSizePriorityQueue} with the specified
	 * {@code maxSize} and {@code comparator}.
	 *
	 * @param maxSize
	 *            - The maximum size the queue can reach, must be a positive
	 *            integer.
	 * @param comparator
	 *            - The comparator to be used to compare the elements in the
	 *            queue, must be non-null.
	 */
	public FixedSizePriorityQueue(final int maxSize,
			final Comparator<? super T> comparator) {
		super(maxSize, comparator); // initialCapacity is set to maxSize, as it
		// will always reach this size
		if (maxSize <= 0) {
			throw new IllegalArgumentException("maxSize = " + maxSize
					+ "; expected a positive integer.");
		}
		if (comparator == null) {
			throw new NullPointerException("Comparator is null.");
		}
		_maxSize = maxSize;
	}

	/**
	 * Adds an element to the queue. If the queue contains {@code maxSize}
	 * elements, {@code e} will be compared to the lowest element in the queue
	 * using {@code comparator}. If {@code e} is greater than or equal to the
	 * lowest element, that element will be removed and {@code e} will be added
	 * instead. Otherwise, the queue will not be modified and {@code e} will not
	 * be added.
	 *
	 * @param e
	 *            - Element to be added, must be non-null.
	 */
	@Override
	public boolean add(final T e) {
		if (e == null) {
			throw new NullPointerException("e is null.");
		}
		if (_maxSize <= size()) {
			final T firstElm = peek();
			if (super.comparator().compare(e, firstElm) < 1) {
				// e.priority is less than the smallest priority in the queue;
				// not added
				return false;
			} else {
				// remove the element with the smallest priority to provide
				// space for the new element
				poll();
			}
		}
		super.add(e);
		return true;
	}
}
