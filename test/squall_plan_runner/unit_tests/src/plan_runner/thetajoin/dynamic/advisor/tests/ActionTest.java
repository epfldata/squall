package plan_runner.thetajoin.dynamic.advisor.tests;



import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

import ch.epfl.data.plan_runner.thetajoin.dynamic.advisor.Action;



public class ActionTest {
	
	@Test
	public void testGetters() {
		int rows = new Random().nextInt();
		int cols = new Random().nextInt();
		Action action = new DummyAction(16, 1, 1, rows, cols);
		assertEquals(action.getNewRows(), rows);
		assertEquals(action.getNewColumns(), cols);
		assertEquals(action.getPreviousRows(), 1);
		assertEquals(action.getPreviousColumns(), 1);
	}
	
	private class DummyAction extends Action {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public DummyAction(int reducerCount, int previousRows,
				int previousColumns, int newRows, int newColumns) {
			super(reducerCount, previousRows, previousColumns, newRows, newColumns);
		}

		@Override
		protected void process() {
		}

		@Override
		public int getNewReducerName(int oldId) {
			return 0;
		}

		@Override
		public int getDiscardRowSplits() {
			return 0;
		}

		@Override
		public int getDiscardColumnSplits() {
			return 0;
		}

		@Override
		public int getRowKeptPieceIndex(int oldId) {
			return 0;
		}

		@Override
		public int getColumnKeptPieceIndex(int oldId) {
			return 0;
		}

		@Override
		public int[] getRowExchangeReducers(int oldId) {
			return null;
		}

		@Override
		public int[] getColumnExchangeReducers(int oldId) {
			return null;
		}

		@Override
		public String toString() {
			return "";
		}

		@Override
		public int getOldReducerName(int newId) {
			// TODO Auto-generated method stub
			return 0;
		}
		
	}
}
