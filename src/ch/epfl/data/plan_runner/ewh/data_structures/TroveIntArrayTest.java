package ch.epfl.data.plan_runner.ewh.data_structures;

import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.Collections;

import ch.epfl.data.plan_runner.utilities.MyUtilities;

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
