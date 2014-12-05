package plan_runner.thetajoin.dynamic.advisor.tests;



import static org.junit.Assert.*;


import java.util.Random;

import org.junit.Test;

import plan_runner.thetajoin.dynamic.advisor.Maybe;



public class MaybeTest {

	@Test
	public void testNone() {
		Maybe<?> m = new Maybe<Integer>();
		assertTrue(m.isNone());
		assertNull(m.get());
	}
	
	@Test
	public void testValue() {
		int value = new Random().nextInt();
		Maybe<Integer> m = new Maybe<Integer>(value);
		assertFalse(m.isNone());
		assertNotNull(m.get());
		assertEquals(m.get().intValue(), value);
	}
}
