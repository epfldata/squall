package thetajoin.matrixMapping;

import java.util.ArrayList;


public interface MatrixAssignment {
	
	public enum Dimension {
	    ROW, COLUMN 
	};
	/**
	 * This method is used to get a list of reducers to which a given tuple must be send. 
	 * @param RowOrColumn indicate from which relation the tuple comes.
	 * @return a list of index of reducers.
	 */
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn);
}
