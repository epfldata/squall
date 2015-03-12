package plan_runner.thetajoin.dynamic.advisor.tests;



import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import ch.epfl.data.plan_runner.thetajoin.dynamic.advisor.Discard;



public class DiscardPivotTest extends Discard {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static int ARRAY_SIZE = 1000;
/*
	private void testPivoting(int[] arr, int[] arrCopy, int from, int to, int p) {
		int pos = pivot(arr, p, from, to);
		
		assertTrue(pos >= from && pos <= to);
		
		assertEquals(arrCopy[p], arr[pos]);
		
		for (int k = 0; k < from; ++k)
			assertEquals(arrCopy[k], arr[k]);
		
		for (int k = from; k < pos; ++k)
			assertTrue(arr[k] < arr[pos]);
		for (int k = pos + 1; k <= to; ++k)
			assertTrue(arr[k] >= arr[pos]);
		
		Arrays.sort(arr);
		Arrays.sort(arrCopy);
		
		assertArrayEquals(arr, arrCopy);
	}
	
	@Test
	public void testPivot() {
		Random random = new Random();
		for (int i = 0; i < 10; ++i) {
			int size = 1 + random.nextInt(ARRAY_SIZE);
			int[] arr = new int[size];
			int[] arrCopy = new int[size];
			
			for (int k = 0; k < size; ++k)
				arrCopy[k] = arr[k] = random.nextInt();
			
			int p = random.nextInt(size);
			int from = p == 0 ? p : random.nextInt(p);
			int to = from + random.nextInt(size - from);
			
			testPivoting(arr, arrCopy, from, to, p);
		}
	}

	@Test
	public void testPivotFirstElement() {
		Random random = new Random();
		int size = 1 + random.nextInt(ARRAY_SIZE);
		int[] arr = new int[size];
		int[] arrCopy = new int[size];
		
		for (int k = 0; k < size; ++k)
			arrCopy[k] = arr[k] = random.nextInt();
		
		int p = random.nextInt(size);
		int from = p;
		int to = from + random.nextInt(size - from);
		
		testPivoting(arr, arrCopy, from, to, p);
	}
	
	@Test
	public void testPivotLastElement() {
		Random random = new Random();
		int size = 1 + random.nextInt(ARRAY_SIZE);
		int[] arr = new int[size];
		int[] arrCopy = new int[size];
		
		for (int k = 0; k < size; ++k)
			arrCopy[k] = arr[k] = random.nextInt();
		
		int p = random.nextInt(size);
		int from = p == 0 ? p : random.nextInt(p);
		int to = p;
		
		testPivoting(arr, arrCopy, from, to, p);
	}
	
	@Test
	public void testPivotSingleElement() {
		Random random = new Random();
		int size = 1 + random.nextInt(ARRAY_SIZE);
		int[] arr = new int[size];
		int[] arrCopy = new int[size];
		
		for (int k = 0; k < size; ++k)
			arrCopy[k] = arr[k] = random.nextInt();
		
		int p = random.nextInt(size);
		int from = p;
		int to = p;
		
		testPivoting(arr, arrCopy, from, to, p);
	}
*/
}
