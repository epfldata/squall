package plan_runner.ewh.algorithms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import plan_runner.ewh.data_structures.JoinMatrix;
import plan_runner.ewh.data_structures.Region;
import plan_runner.utilities.MyUtilities;

public abstract class ShallowCoarsener {
	protected JoinMatrix _originalMatrix;
	
	public abstract void setOriginalMatrix(JoinMatrix originalMatrix, StringBuilder sb);
	public abstract WeightPrecomputation getPrecomputation();
	
	public abstract int getNumXCoarsenedPoints();
	public abstract int getNumYCoarsenedPoints();
	
	public abstract int getOriginalXCoordinate(int ci, boolean isHigher);
	public abstract int getOriginalYCoordinate(int cj, boolean isHigher);
	
	public abstract int getCoarsenedXCoordinate(int x);
	public abstract int getCoarsenedYCoordinate(int y);	
	
	public Region translateCoarsenedToOriginalRegion(Region coarsenedRegion) {
		int cx1 = coarsenedRegion.get_x1();
		int cy1 = coarsenedRegion.get_y1();
		int cx2 = coarsenedRegion.get_x2();
		int cy2 = coarsenedRegion.get_y2();
		
		int x1 = getOriginalXCoordinate(cx1, false);
		int y1 = getOriginalYCoordinate(cy1, false);
		int x2 = getOriginalXCoordinate(cx2, true);
		int y2 = getOriginalYCoordinate(cy2, true);

		return new Region(x1, y1, x2, y2);
	}
	
	public Region translateOriginalToCoarsenedRegion(Region originalRegion) {
		int x1 = originalRegion.get_x1();
		int y1 = originalRegion.get_y1();
		int x2 = originalRegion.get_x2();
		int y2 = originalRegion.get_y2();
		
		int cx1 = getCoarsenedXCoordinate(x1);
		int cy1 = getCoarsenedYCoordinate(y1);
		int cx2 = getCoarsenedXCoordinate(x2);
		int cy2 = getCoarsenedYCoordinate(y2);
		
		return new Region(cx1, cy1, cx2, cy2);
	}
	
	// monotonic optimization not worthed - for jps, it takes only 0.008 for p = 128
	public List<Region> getCandidateRoundedCells(Map conf) {
		List<Region> regions = new ArrayList<Region>();
		for(int i = 0; i < getNumXCoarsenedPoints(); i++){
			int x1 = getOriginalXCoordinate(i, false);
			int x2 = getOriginalXCoordinate(i, true);
			for(int j = 0; j < getNumYCoarsenedPoints(); j++){
				int y1 = getOriginalYCoordinate(j, false);
				int y2 = getOriginalYCoordinate(j, true);
				Region region = new Region(x1, y1, x2, y2);

				if(MyUtilities.isCandidateRegion(_originalMatrix, region, _originalMatrix.getComparisonPredicate(), conf)){
					regions.add(region);
				}
			}
		}
		return regions;
	}

	// monotonic optimization not worthed - for jps, it takes only 0.008 for p = 128
	public int getNumCandidateRoundedCells(Map conf) {
		int result = 0;
		for(int i = 0; i < getNumXCoarsenedPoints(); i++){
			int x1 = getOriginalXCoordinate(i, false);
			int x2 = getOriginalXCoordinate(i, true);
			for(int j = 0; j < getNumYCoarsenedPoints(); j++){
				int y1 = getOriginalYCoordinate(j, false);
				int y2 = getOriginalYCoordinate(j, true);
				Region region = new Region(x1, y1, x2, y2);

				if(MyUtilities.isCandidateRegion(_originalMatrix, region, _originalMatrix.getComparisonPredicate(), conf)){
					result++;
				}
			}
		}
		return result;
	}
}
