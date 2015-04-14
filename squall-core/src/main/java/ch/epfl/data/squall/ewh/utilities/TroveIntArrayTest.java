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

package ch.epfl.data.squall.ewh.utilities;

import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.Collections;

import ch.epfl.data.squall.utilities.MyUtilities;

public class TroveIntArrayTest {

    private final static int SIZE = 4000000;

    public static void main(String[] args) {
	testTroveArray();
	// testJavaArray();

	/*
	 * Trove SIZE = 4M time = 0.133s memory = 41MBs Java SIZE = 4M time =
	 * 1.054s memory = 92MBs Trove SIZE = 8M time = 0.176s memory = 81MBs
	 * Java SIZE = 8M time = 1.95s memory = 212MBs
	 * 
	 * CONCLUSION: trove is 10 times faster and 2 times more efficient in
	 * memory
	 */
    }

    private static void testTroveArray() {
	long startTime = System.currentTimeMillis();
	TIntArrayList troveIntList = new TIntArrayList();
	for (int i = 0; i < SIZE; i++) {
	    troveIntList.add(i);
	    // with troveIntList.add((Integer)i); is up to 2X slower (still,
	    // it's at least 5X faster than java implementation)
	}
	troveIntList.sort();
	long elapsed = System.currentTimeMillis() - startTime;
	System.out.println("Creating and sorting TroveIntList takes " + elapsed
		/ 1000.0 + " seconds and " + MyUtilities.getUsedMemoryMBs()
		+ " MBs.");
    }

    private static void testJavaArray() {
	long startTime = System.currentTimeMillis();
	ArrayList<Integer> intList = new ArrayList<Integer>();
	for (int i = 0; i < SIZE; i++) {
	    intList.add(i);
	}
	Collections.sort(intList);
	long elapsed = System.currentTimeMillis() - startTime;
	System.out.println("Creating and sorting JavaIntList takes " + elapsed
		/ 1000.0 + " seconds and " + MyUtilities.getUsedMemoryMBs()
		+ " MBs.");
    }
}
