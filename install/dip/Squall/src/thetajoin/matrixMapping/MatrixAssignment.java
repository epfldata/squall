package thetajoin.matrixMapping;

import java.util.ArrayList;


public interface MatrixAssignment {
	
	public enum Dimension {
	    ROW, COLUMN 
	};
	
	public ArrayList<Integer> getRegionIDs(Dimension RowOrColumn);
	public int getNumberOfWorkerRows();
	public int getNumberOfWorkerColumns();
}
