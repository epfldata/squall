package plan_runner.thetajoin.dynamic.advisor.tests;



import static org.junit.Assert.*;

import org.junit.Test;

import ch.epfl.data.plan_runner.thetajoin.dynamic.advisor.Migration;


public class MigrationDiscardTest {

	@Test
	public void testRowSplit() {
		Migration migration = new Migration(64, 2, 32, 8, 8);
		assertEquals(4, migration.getDiscardRowSplits());
		
		for (int i = 0; i < 64; ++i)
			assertEquals(i % 32 / 8, migration.getRowKeptPieceIndex(i));
	}
	
	@Test
	public void testRowNoSplit() {
		Migration migration = new Migration(64, 2, 32, 2, 32);
		assertEquals(1, migration.getDiscardRowSplits());
		
		for (int i = 0; i < 64; ++i)
			assertEquals(0, migration.getRowKeptPieceIndex(i));
		
		migration = new Migration(64, 2, 32, 1, 64);
		assertEquals(1, migration.getDiscardRowSplits());
		
		for (int i = 0; i < 64; ++i)
			assertEquals(0, migration.getRowKeptPieceIndex(i));
	}
	
	@Test
	public void testColumnSplit() {
		Migration migration = new Migration(64, 32, 2, 8, 8);
		assertEquals(4, migration.getDiscardColumnSplits());
		
		for (int i = 0; i < 64; ++i) {
			assertEquals(i / 2 / 8, migration.getColumnKeptPieceIndex(i));
		}
	}
	
	@Test
	public void testColumnNoSplit() {
		Migration migration = new Migration(64, 2, 32, 2, 32);
		assertEquals(1, migration.getDiscardColumnSplits());
		
		for (int i = 0; i < 64; ++i)
			assertEquals(0, migration.getColumnKeptPieceIndex(i));
		
		migration = new Migration(64, 1, 64, 2, 32);
		assertEquals(1, migration.getDiscardColumnSplits());
		
		for (int i = 0; i < 64; ++i)
			assertEquals(0, migration.getColumnKeptPieceIndex(i));
	}
	
	@Test
	public void testColumnSplit2() {
		Migration migration = new Migration(16, 4, 4, 2, 8);
		assertEquals(2, migration.getDiscardColumnSplits());
		int[] result = {
				0, 0, 0, 0, 0, 0, 0, 0,
				1, 1, 1, 1, 1, 1, 1, 1
		};
		
		for (int i = 0; i < 16; ++i) {
			assertEquals(result[i], migration.getColumnKeptPieceIndex(i));
		}
	}
}
