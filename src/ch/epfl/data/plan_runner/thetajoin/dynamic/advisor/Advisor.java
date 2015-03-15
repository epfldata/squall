package ch.epfl.data.plan_runner.thetajoin.dynamic.advisor;

import java.io.Serializable;
import java.util.Random;

/**
 * Advisor class that provides decision s on reducer assignment.
 */
public abstract class Advisor implements Serializable {

	public static int[] getAssignedReducers(boolean isRow, int reducerCount,
			int currentRows, int currentColumns) {
		int[] result;
		int k = 0;
		final int reducersPerGroup = reducerCount / currentRows
				/ currentColumns;
		if (isRow) {
			final int row = gen.nextInt(currentRows);
			result = new int[currentColumns * reducersPerGroup];
			for (int g = row * currentColumns; g < (row + 1) * currentColumns; ++g)
				for (int i = 0; i < reducersPerGroup; ++i)
					result[k++] = g * reducersPerGroup + i;
		} else {
			final int column = gen.nextInt(currentColumns);
			result = new int[currentRows * reducersPerGroup];
			for (int g = column; g < currentRows * currentColumns; g += currentColumns)
				for (int i = 0; i < reducersPerGroup; ++i)
					result[k++] = g * reducersPerGroup + i;
		}
		return result;
	}

	public static boolean isLeader(int reducerCount, int currentRows,
			int currentColumns, int id) {
		final int machinesPerGroup = reducerCount / currentColumns
				/ currentRows;
		return id % machinesPerGroup == 0;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/* Counters */

	// Total number of reducers.
	protected int reducerCount;

	// Current dimensions of the reducer assignment matrix.
	protected int currentRows, currentColumns;

	// Total number of tuples along the row and the column.
	public long totalRowTuples, totalColumnTuples;

	protected long deltaR, deltaS;

	/**
	 * Randomized function that returns the ids of the reducers that should
	 * receive and input tuple.
	 * 
	 * @param isRow
	 *            True if the input tuple is from the relation along the rows,
	 *            false otherwise.
	 * @param reducerCount
	 *            Total number of reducers.
	 * @param currentRows
	 *            Number of rows in the matrix.
	 * @param currentColumns
	 *            Number of columns in the matrix.
	 * @return Ids of the reducers that should receive the tuples.
	 */
	private static Random gen = new Random();

	/**
	 * Use this constructor to start with the default initial assignment.
	 * 
	 * @param reducerCount
	 *            Total number of reducers.
	 */
	public Advisor(int reducerCount) {
		this(reducerCount, 1, 1);
	}

	/**
	 * Use this constructor when you want to start with a specific input matrix.
	 * 
	 * @param reducerCount
	 *            Total number of reducers.
	 * @param initialRows
	 *            Number of row splits.
	 * @param initialColumns
	 *            Number of column splits.
	 */
	public Advisor(int reducerCount, int initialRows, int initialColumns) {
		this.reducerCount = reducerCount;
		currentRows = initialRows;
		currentColumns = initialColumns;
	}

	/**
	 * This is the main method to call. This returns the action to be taken.
	 * 
	 * @return The action to take.
	 */
	public Maybe<Action> advise() {
		if (currentRows * currentColumns < reducerCount)
			// In splitting phase
			return doSplit();
		else
			// Splitting is done, consider migration.
			return doMigration();
	}

	/**
	 * Same as {@link advise} but also updates the matrix assignment.
	 * 
	 * @return The action to take.
	 */
	public Maybe<Action> adviseAndUpdateDimensions() {
		final Maybe<Action> result = advise();
		if (!result.isNone())
			updateMapping(result.get());
		return result;
	}

	protected abstract Maybe<Action> doMigration();

	protected abstract Maybe<Action> doSplit();

	/**
	 * @param action
	 *            The action including the new dimensions.
	 */
	public void updateMapping(Action action) {
		currentRows = action.getNewRows();
		currentColumns = action.getNewColumns();
	}

	/**
	 * This updates the instance with the arrival of tuples.
	 * 
	 * @param rowTuples
	 *            Number of tuples from the relation along the rows.
	 * @param columnTuples
	 *            Number of tuples from the relation along the columns.
	 */
	public void updateTuples(long rowTuples, long columnTuples) {
		totalRowTuples += rowTuples;
		deltaR += rowTuples;
		totalColumnTuples += columnTuples;
		deltaS += columnTuples;
	}
}