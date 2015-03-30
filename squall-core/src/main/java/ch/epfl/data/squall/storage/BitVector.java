package ch.epfl.data.squall.storage;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Optimized implementation of a vector of bits. This is more-or-less like
 * java.util.BitSet, but also includes the following: a count() method, which
 * efficiently computes the number of one bits; optimized read from and write to
 * disk; inlinable get() method;
 * 
 * @author Doug Cutting
 * @version $Id: BitVector.java,v 1.4 2004/03/29 22:48:05 cutting Exp $
 */
public final class BitVector {

	public static void main(String[] args) {
		final BitVector bb = new BitVector(6);
		bb.set(3);
		bb.set(1);
		System.out.println(bb.get(1));
		System.out.println(bb.get(7));
		System.out.println(bb.get(3));
		bb.clear(3);
		System.out.println(bb.get(3));

	}

	private final byte[] bits;
	private final int size;

	private int count = -1;

	private static final byte[] BYTE_COUNTS = { // table of bits/byte
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2,
			3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4,
			5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2,
			3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
			5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3,
			4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3,
			4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4,
			5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4,
			5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3,
			4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6,
			7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5,
			6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8 };

	/** Constructs a vector capable of holding n bits. */
	public BitVector(int n) {
		size = n;
		bits = new byte[(size >> 3) + 1];
	}

	/** Sets the value of bit to zero. */
	public final void clear(int bit) {
		bits[bit >> 3] &= ~(1 << (bit & 7));
		count = -1;
	}

	/**
	 * Returns the total number of one bits in this vector. This is efficiently
	 * computed and cached, so that, if the vector is not changed, no
	 * recomputation is done for repeated calls.
	 */
	public final int count() {
		// if the vector has been modified
		if (count == -1) {
			int c = 0;
			final int end = bits.length;
			for (int i = 0; i < end; i++)
				c += BYTE_COUNTS[bits[i] & 0xFF]; // sum bits per byte
			count = c;
		}
		return count;
	}

	/**
	 * Returns true if bit is one and false if it is zero.
	 */
	public final boolean get(int bit) {
		return (bits[bit >> 3] & (1 << (bit & 7))) != 0;
	}

	/** Sets the value of bit to one. */
	public final void set(int bit) {
		bits[bit >> 3] |= 1 << (bit & 7);
		count = -1;
	}

	/** Sets the value of bit to one. */
	public final void set(int bit, boolean bool) {
		if (bool == true)
			set(bit);
		else
			clear(bit);

	}

	public final void set(int from, int to) {
		for (int i = from; i < to; i++)
			set(i);
	}

	/**
	 * Returns the number of bits in this vector. This is also one greater than
	 * the number of the largest valid bit number.
	 */
	public final int size() {
		return size;
	}

}