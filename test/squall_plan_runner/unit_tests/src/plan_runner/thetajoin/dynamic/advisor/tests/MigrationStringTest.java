package plan_runner.thetajoin.dynamic.advisor.tests;



import static org.junit.Assert.*;

import org.junit.Test;

import ch.epfl.data.plan_runner.thetajoin.dynamic.advisor.Action;
import ch.epfl.data.plan_runner.thetajoin.dynamic.advisor.Migration;


public class MigrationStringTest {

	@Test
	public void testIdentity() {
		Migration migration = new Migration(4, 1, 1, 2, 2);
		assertTrue(migration.equals(Action.fromString(migration.toString())));
	}

}
