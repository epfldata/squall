package plan_runner.thetajoin.dynamic.advisor.tests;



import static org.junit.Assert.*;

import org.junit.Test;

import ch.epfl.data.plan_runner.thetajoin.dynamic.advisor.Migration;


public class MigrationExchangeTest {

	@Test
	public void testRowNoDiscard() {
		Migration migration = new Migration(16, 4, 4, 4, 4);
		for (int i = 0; i < 16; ++i)
			assertArrayEquals(new int[] {}, migration.getRowExchangeReducers(i));
	}
	
	@Test
	public void testRowDiscard() {
		Migration migration = new Migration(16, 16, 1, 4, 4);
		int[][] results = {
				{4, 8, 12}, {5, 9, 13}, {6, 10, 14}, {7, 11, 15},
				{0, 8, 12}, {1, 9, 13}, {2, 10, 14}, {3, 11, 15},
				{0, 4, 12}, {1, 5, 13}, {2, 6, 14}, {3, 7, 15},
				{0, 4, 8}, {1, 5, 9}, {2, 6, 10}, {3, 7, 11}
		};
		
		for (int i = 0; i < 16; ++i) {
			int[] exchanges = migration.getRowExchangeReducers(i);
			assertEquals(results[i].length, exchanges.length);
			for (int j = 0; j < results[i].length; ++j)
				assertEquals(results[i][j], exchanges[j]);
		}
	}
	
	@Test
	public void testRowDiscard2() {
		Migration migration = new Migration(4, 4, 1, 2, 2);
		int[][] results = {
				{1}, {0}, {3}, {2}
		};
		
		for (int i = 0; i < 4; ++i) {
			int[] exchanges = migration.getRowExchangeReducersByNewId(i);
			assertEquals(results[i].length, exchanges.length);
			for (int j = 0; j < results[i].length; ++j)
				assertEquals(results[i][j], exchanges[j]);
		}
	}

	@Test
	public void testColumnNoDiscard() {
		Migration migration = new Migration(16, 4, 4, 4, 4);
		for (int i = 0; i < 16; ++i)
			assertArrayEquals(new int[] {}, migration.getColumnExchangeReducers(i));
	}
	
	@Test
	public void testColumnDiscard() {
		Migration migration = new Migration(16, 1, 16, 4, 4);
		int[][] results = {
				{4, 8, 12}, {5, 9, 13}, {6, 10, 14}, {7, 11, 15},
				{0, 8, 12}, {1, 9, 13}, {2, 10, 14}, {3, 11, 15},
				{0, 4, 12}, {1, 5, 13}, {2, 6, 14}, {3, 7, 15},
				{0, 4, 8}, {1, 5, 9}, {2, 6, 10}, {3, 7, 11}
		};
		
		for (int i = 0; i < 16; ++i) {
			int[] exchanges = migration.getColumnExchangeReducers(i);
			assertEquals(results[i].length, exchanges.length);
			for (int j = 0; j < results[i].length; ++j)
				assertEquals(results[i][j], exchanges[j]);
			//for (int e : exchanges) System.err.print(e + " ");
			//System.err.println();
		}
	}
}
