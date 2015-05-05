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

/**
 * @author ElSeidy
 *  Utilities class used for mapping.
 */

package ch.epfl.data.squall.thetajoin.matrix_assignment;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utilities {

    /**
     * Multiplies the contents of the list
     * 
     * @param List
     *            of numbers
     * @return The multiplication output of the
     */
    public static int multiply(List<Integer> list) {
	int mult = 1;
	for (final Integer value : list)
	    mult *= value;
	return mult;
    }

    /**
     * Generates the power set
     * 
     * @param Set
     *            of numbers
     * @return Set of sets
     */
    public static <T> Set<List<T>> powerSet(List<T> originalSet) {
	final Set<List<T>> sets = new HashSet<List<T>>();
	if (originalSet.isEmpty()) {
	    sets.add(new ArrayList<T>());
	    return sets;
	}
	final List<T> list = new ArrayList<T>(originalSet);
	final T head = list.get(0);
	final List<T> rest = new ArrayList<T>(list.subList(1, list.size()));
	for (final List<T> set : powerSet(rest)) {
	    final List<T> newSet = new ArrayList<T>();
	    newSet.add(head);
	    newSet.addAll(set);
	    sets.add(newSet);
	    sets.add(set);
	}
	return sets;
    }

    /**
     * Gets the prime factors for any integer.
     * 
     * @param any
     *            number
     * @return the prime factors
     */

    public static List<Integer> primeFactors(int numbers) {
	int n = numbers;
	final List<Integer> factors = new ArrayList<Integer>();
	for (int i = 2; i <= n / i; i++)
	    while (n % i == 0) {
		factors.add(i);
		n /= i;
	    }
	if (n > 1)
	    factors.add(n);
	return factors;
    }

    /**
     * Compute the difference between two lists. For example [2,3,2,4] - [2,4] =
     * [3,2]
     *
     * @param l1
     *            list to be removed from
     * @param l2
     *            list of elements to remove
     */
    public static void removeOnce(List<Integer> l1, List<Integer> l2) {
        for (Integer removeElement : l2) {
            l1.remove(removeElement);
        }
    }

    /**
     * Convenience method to convert from unspecified number of parameters into an array of parameters
     */
    public static long[] arrayOf(long... values) {
        return values;
    }

    /**
     * Copy each element of source array into destination array
     */
    public static void copy(int[] source, int[] des) {
        for (int i = 0; i < source.length; i++) {
            des[i] = source[i];
        }
    }

    /**
     * Return the product over all elements of an array
     */
    public static int multiply(int[] a) {
        int mult = 1;
        for (final int value : a)
            mult *= value;
        return mult;
    }

    /**
     * Check if an integer is prime
     */
    public static boolean isPrime(int num) {
        if (num % 2 == 0)
            return false;
        for (int i = 3; i * i <= num; i += 2)
            if (num % i == 0)
                return false;
        return true;
    }

    /**
     * Return the index of maximum value of an array
     */
    public static int indexOfMax(double[] a) {
        int index = 0;
        for (int i = 1; i < a.length; i++) {
            if (a[i] > a[index])
                index = i;
        }
        return index;
    }

    /**
     * Check whether an array contains an element with value less than v.
     */
    public static boolean existsLess(double[] a, double v) {
        for (double ai : a) {
            if (ai < v)
                return true;
        }
        return false;
    }
}
