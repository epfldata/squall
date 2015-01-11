package ch.epfl.data.plan_runner.thetajoin.matrix_mapping;

import java.util.ArrayList;

public interface MatrixAssignment<KeyType> {

	public enum Dimension {
		ROW, COLUMN
	};

	/**
	 * This method is used to get a list of reducers to which a given tuple must
	 * be sent.
	 * 
	 * @param RowOrColumn
	 *            indicate from which relation the tuple comes.
	 * @return a list of index of reducers.
	 */
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn);
	
	/**
	 * This method is used to get a list of reducers to which a given tuple must
	 * be sent to given a key.
	 * 
	 * @param RowOrColumn
	 *            indicate from which relation the tuple comes.
	 * @return a list of index of reducers.
	 */
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn, KeyType key);
	
	
}
