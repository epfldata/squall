package ch.epfl.data.plan_runner.ewh.data_structures;

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