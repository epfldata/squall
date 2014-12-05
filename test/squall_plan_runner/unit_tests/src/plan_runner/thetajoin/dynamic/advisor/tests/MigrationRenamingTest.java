package plan_runner.thetajoin.dynamic.advisor.tests;



import static org.junit.Assert.*;

import org.junit.Test;

import plan_runner.thetajoin.dynamic.advisor.Migration;



public class MigrationRenamingTest {

	@Test
	public void testRenamingIdentity() {
		Migration identity = new Migration(64, 8, 8, 8, 8);
		for (int i = 0; i < 64; ++i)
			assertEquals(i, identity.getNewReducerName(i));
	}
	
	@Test
	public void testRenamingMoreRows() {
		int[] newIds = {0, 1, 4, 5, 2, 3, 6, 7};
		Migration migration = new Migration(8, 2, 4, 4, 2);
		for (int i = 0; i < 8; ++i)
			assertEquals(newIds[i], migration.getNewReducerName(i));
	}
	
	@Test
	public void testRenamingLessRows() {
		int[] newIds = {0, 1, 4, 5, 2, 3, 6, 7};
		Migration migration = new Migration(8, 4, 2, 2, 4);
		for (int i = 0; i < 8; ++i)
			assertEquals(newIds[i], migration.getNewReducerName(i));
	}
	
	@Test
	public void testRenamingAndBack() {
		Migration migration = new Migration(8, 4, 2, 2, 4);
		for (int i = 0; i < 8; ++i)
			assertEquals(i, migration.getOldReducerName(migration.getNewReducerName(i)));
	}
	
	@Test
	public void testInvertible() {
		Migration migration = new Migration(8, 2, 4, 4, 2);
		Migration inverse = new Migration(8, 4, 2, 2, 4);
		for (int i = 0; i < 8; ++i)
			assertEquals(i, inverse.getNewReducerName(migration.getNewReducerName(i)));
	}
	
	@Test
	public void testRowMatrix() {
		Migration migration = new Migration(8, 1, 8, 4, 2);
		for (int i = 0; i < 8; ++i)
			assertEquals(i, migration.getNewReducerName(i));
	}
	
	@Test
	public void testColumnMatrix() {
		int[] newIds = {0, 2, 4, 6, 1, 3, 5, 7};
		Migration migration = new Migration(8, 8, 1, 4, 2);
		for (int i = 0; i < 8; ++i) {
			assertEquals(newIds[i], migration.getNewReducerName(i));
		}
	}
	
	@Test
	public void testTwoRenamings() {
		Migration migration;
		
		migration = new Migration(4, 4, 1, 2, 2);
		assertEquals(0, migration.getNewReducerName(0));
		assertEquals(2, migration.getNewReducerName(1));
		assertEquals(1, migration.getNewReducerName(2));
		assertEquals(3, migration.getNewReducerName(3));
		
		migration = new Migration(4, 2, 2, 1, 4);
		assertEquals(0, migration.getNewReducerName(0));
		assertEquals(1, migration.getNewReducerName(1));
		assertEquals(2, migration.getNewReducerName(2));
		assertEquals(3, migration.getNewReducerName(3));
	}
}
