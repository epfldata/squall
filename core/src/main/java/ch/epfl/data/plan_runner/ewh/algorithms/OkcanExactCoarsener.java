package ch.epfl.data.plan_runner.ewh.algorithms;

import java.util.Map;
import java.util.logging.Logger;

import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;
import ch.epfl.data.plan_runner.ewh.data_structures.UJMPAdapterIntMatrix;
import ch.epfl.data.plan_runner.utilities.MyUtilities;

// this class makes a hybrid between Okcan and us:
//   it uses Okcan heuristics, but keeps the exact information about the output
public class OkcanExactCoarsener extends OkcanCandidateCoarsener {
	private static Logger LOG = Logger.getLogger(OkcanExactCoarsener.class);

	@Override
	public JoinMatrix createAndFillCoarsenedMatrix(JoinMatrix originalMatrix,
			int numXBuckets, int numYBuckets, Map map) {
		setParameters(originalMatrix, numXBuckets, numYBuckets, map);

		JoinMatrix coarsenedMatrix = new UJMPAdapterIntMatrix(_numXBuckets,
				_numYBuckets, _map);
		LOG.info("Capacity of coarsened joinMatrix in OkcanExactCoarsener is "
				+ coarsenedMatrix.getCapacity());
		int firstCandInLastLine = 0;
		for (int i = 0; i < _numXBuckets; i++) {
			boolean isFirstInLine = true;
			int x1 = getOriginalXCoordinate(i, false);
			int x2 = getOriginalXCoordinate(i, true);
			for (int j = firstCandInLastLine; j < _numYBuckets; j++) {
				int y1 = getOriginalYCoordinate(j, false);
				int y2 = getOriginalYCoordinate(j, true);
				// LOG.info("x1 = " + x1 + ", y1 = " + y1 + ", x2 = " + x2 +
				// ", y2 = " + y2);
				Region region = new Region(x1, y1, x2, y2);
				boolean isCandidate = MyUtilities.isCandidateRegion(
						originalMatrix, region, _cp, _map);
				if (isCandidate) {
					int frequency = originalMatrix
							.getRegionNumOutputs(new Region(x1, y1, x2, y2));
					if (frequency == 0) {
						frequency = 1;
						// it's a candidate, so let's assign to it a
						// minPositiveValue
					}
					coarsenedMatrix.setElement(frequency, i, j);
					if (isFirstInLine) {
						firstCandInLastLine = j;
						isFirstInLine = false;
					}
				}
				if (!isFirstInLine && !isCandidate) {
					// I am right from the candidate are; the first
					// non-candidate guy means I should switch to the next row
					break;
				}
			}
		}
		return coarsenedMatrix;
	}
}
