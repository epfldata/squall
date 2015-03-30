package ch.epfl.data.squall.ewh.algorithms;

import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.ewh.data_structures.JoinMatrix;
import ch.epfl.data.squall.ewh.data_structures.Region;

public interface OkcanCoarsener {
	public JoinMatrix createAndFillCoarsenedMatrix(JoinMatrix originalMatrix,
			int numXBuckets, int numYBuckets, Map map);

	public int getOriginalXCoordinate(int cx, boolean isHigher);

	public int getOriginalXSize();

	public int getOriginalYCoordinate(int cy, boolean isHigher);

	public int getOriginalYSize();

	public Region translateCoarsenedToOriginalRegion(Region coarsenedRegion);

	public List<Region> translateCoarsenedToOriginalRegions(
			List<Region> coarsenedRegions);
}
