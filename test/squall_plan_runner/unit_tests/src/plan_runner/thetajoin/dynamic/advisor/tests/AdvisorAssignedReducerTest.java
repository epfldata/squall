package plan_runner.thetajoin.dynamic.advisor.tests;



import static org.junit.Assert.*;


import org.junit.Test;

import plan_runner.thetajoin.dynamic.advisor.Advisor;



public class AdvisorAssignedReducerTest {
	
	@Test
	public void testTrivial() {
		int[] reducers;
		
		reducers = Advisor.getAssignedReducers(true, 1, 1, 1);
		assertArrayEquals(new int[] {0}, reducers);
		
		reducers = Advisor.getAssignedReducers(false, 1, 1, 1);
		assertArrayEquals(new int[] {0}, reducers);
	}
	
	@Test
	public void testRowAssignment() {
		int[] reducers = Advisor.getAssignedReducers(true, 4, 2, 2);
		assertEquals(2, reducers.length);
		boolean case1 = reducers[0] == 0 && reducers[1] == 2;
		boolean case2 = reducers[0] == 1 && reducers[1] == 3;
		assert(case1 || case2);
	}
	
	@Test
	public void testColumnAssignment() {
		int[] reducers = Advisor.getAssignedReducers(false, 4, 2, 2);
		assertEquals(2, reducers.length);
		boolean case1 = reducers[0] == 0 && reducers[1] == 1;
		boolean case2 = reducers[0] == 2 && reducers[1] == 3;
		assert(case1 || case2);
	}
}
