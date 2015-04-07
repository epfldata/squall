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


package ch.epfl.data.squall.thetajoin.adaptive.advisor;

import java.io.Serializable;

import ch.epfl.data.squall.storage.BitVector;

/**
 * Utility class to efficiently simulate tuple discards.
 */
public class Discard implements Serializable {

	/**
	 * This method divides the array into k parts and returns the ith part of
	 * the sorted array. The method operates in place and returns two indices
	 * (start and end, both inclusive). The array is not sorted after the method
	 * is called, but the range [start, end] contains the same elements as
	 * [start, end] in the sorted array. This method operates in expected linear
	 * time. Worst case time is quadratic if the array is close to sorted (or
	 * reverse sorted). Works best if the input array is in random order. The
	 * size of a single part is ceil(arr.length / parts). The last few parts
	 * might be empty.
	 * 
	 * @param arr
	 *            The input array.
	 * @param parts
	 *            The number of parts the array should be divided into.
	 * @param index
	 *            The index of the required part, should be in [0, parts)
	 * @return An array containing two indices (start and end), both are
	 *         inclusive. If the required part is empty, the method will return
	 *         {arr.length, arr.length - 1}.
	 */
	public static int[] keep(int[] arr, int[] addresses, BitVector isTagged,
			int parts, int index) {
		final int size = (arr.length + parts - 1) / parts;
		final int from = index * size;
		final int to = Math.min((index + 1) * size - 1, arr.length - 1);
		if (from > to)
			return new int[] { arr.length, arr.length - 1 };
		return keepFrom(arr, addresses, isTagged, from, to, 0, arr.length - 1);
	}

	protected static int[] keepFrom(int[] arr, int[] addresses,
			BitVector isTagged, int from, int to, int i, int j) {
		while (true) {
			final int p = pivot(arr, addresses, isTagged, i, i, j);

			if (from == i)
				return new int[] {
						i,
						keepUntil(arr, addresses, isTagged, to, i,
								arr.length - 1) };

			if (p < from)
				i = p + 1;
			else if (p == from)
				return new int[] {
						p,
						keepUntil(arr, addresses, isTagged, to, i,
								arr.length - 1) };
			else
				j = p - 1;
		}
	}

	protected static int keepUntil(int[] arr, int[] addresses,
			BitVector isTagged, int to, int i, int j) {
		while (true) {
			final int p = pivot(arr, addresses, isTagged, i, i, j);

			if (p > to)
				j = p - 1;
			else if (p == to)
				return p;
			else
				i = p + 1;
		}
	}

	protected static int pivot(int[] arr, int[] addresses, BitVector isTagged,
			int p, int i, int j) {
		final int pV = arr[p];
		final boolean pVB = isTagged.get(p);
		final int pA = addresses[p];

		int temp = arr[j];
		boolean tempB = isTagged.get(j);
		int tempA = addresses[j];

		arr[j] = pV;
		isTagged.set(j, pVB);
		addresses[j] = pA;

		arr[p] = temp;
		isTagged.set(p, tempB);
		addresses[p] = tempA;

		int ind = i;
		for (int k = i; k < j; ++k)
			if (arr[k] < pV) {
				temp = arr[k];
				tempB = isTagged.get(k);
				tempA = addresses[k];

				arr[k] = arr[ind];
				isTagged.set(k, isTagged.get(ind));
				addresses[k] = addresses[ind];

				arr[ind] = temp;
				isTagged.set(ind, tempB);
				addresses[ind] = tempA;
				++ind;
			}
		temp = arr[ind];
		tempB = isTagged.get(ind);
		tempA = addresses[ind];

		arr[ind] = arr[j];
		isTagged.set(ind, isTagged.get(j));
		addresses[ind] = addresses[j];

		arr[j] = temp;
		isTagged.set(j, tempB);
		addresses[j] = tempA;

		return ind;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
}
