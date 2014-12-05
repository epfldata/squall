package plan_runner.thetajoin.dynamic.advisor.tests;



import static org.junit.Assert.*;


import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import plan_runner.thetajoin.dynamic.advisor.Discard;



public class DiscardTest extends Discard {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final int ARRAY_SIZE = 1000;
	/*
	@Test
	public void testKeepSizes() {
		int[] arr;
		
		
		arr = new int[10];
		assertArrayEquals(new int[] {0, 3}, keep(arr, 3, 0));
		assertArrayEquals(new int[] {4, 7}, keep(arr, 3, 1));
		assertArrayEquals(new int[] {8, 9}, keep(arr, 3, 2));

		arr = new int[16];
		assertArrayEquals(new int[] {0, 3}, keep(arr, 4, 0));
		assertArrayEquals(new int[] {4, 7}, keep(arr, 4, 1));
		assertArrayEquals(new int[] {8, 11}, keep(arr, 4, 2));
		assertArrayEquals(new int[] {12, 15}, keep(arr, 4, 3));
		
		arr = new int[16];
		assertArrayEquals(new int[] {0, 1}, keep(arr, 9, 0));
		assertArrayEquals(new int[] {2, 3}, keep(arr, 9, 1));
		assertArrayEquals(new int[] {4, 5}, keep(arr, 9, 2));
		assertArrayEquals(new int[] {6, 7}, keep(arr, 9, 3));
		assertArrayEquals(new int[] {8, 9}, keep(arr, 9, 4));
		assertArrayEquals(new int[] {10, 11}, keep(arr, 9, 5));
		assertArrayEquals(new int[] {12, 13}, keep(arr, 9, 6));
		assertArrayEquals(new int[] {14, 15}, keep(arr, 9, 7));
		assertArrayEquals(new int[] {16, 15}, keep(arr, 9, 8));
		
		arr = new int[19];
		assertArrayEquals(new int[] {0, 4}, keep(arr, 4, 0));
		assertArrayEquals(new int[] {5, 9}, keep(arr, 4, 1));
		assertArrayEquals(new int[] {10, 14}, keep(arr, 4, 2));
		assertArrayEquals(new int[] {15, 18}, keep(arr, 4, 3));
		
		arr = new int[4];
		assertArrayEquals(new int[] {0, 0}, keep(arr, 4, 0));
		assertArrayEquals(new int[] {1, 1}, keep(arr, 4, 1));
		assertArrayEquals(new int[] {2, 2}, keep(arr, 4, 2));
		assertArrayEquals(new int[] {3, 3}, keep(arr, 4, 3));

	}

	@Test
	public void testKeep() {
		
		
		Random random = new Random();
		
		for (int u = 0; u < 1000; ++u) {
			int size = random.nextInt(ARRAY_SIZE);
			int[] arr = new int[size];
			int[] arrCopy = new int[size];
			
			for (int i = 0; i < arr.length; ++i) {
				arr[i] = random.nextInt();
			}
			
			int k = 0;
			int[] bounds;
			
			for (int p = 0; p < 4; ++p) {
				bounds = keep(arr, 4, p);
				int beg = k;
				for (int i = bounds[0]; i <= bounds[1]; ++i) {
					arrCopy[k++] = arr[i];
				}
				Arrays.sort(arrCopy, beg, k);
			}
			assertEquals(size, k);
			
			Arrays.sort(arr);
			
			assertArrayEquals(arr, arrCopy);
		}
		
	}
	*/
}
