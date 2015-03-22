package ch.epfl.data.plan_runner.ewh.data_structures;

import java.util.List;

import ch.epfl.data.plan_runner.ewh.algorithms.DenseWeightPrecomputation;
import ch.epfl.data.plan_runner.ewh.algorithms.ShallowCoarsener;
import ch.epfl.data.plan_runner.ewh.algorithms.WeightPrecomputation;
import ch.epfl.data.plan_runner.ewh.algorithms.optimality.OptimalityMetricInterface;
import ch.epfl.data.plan_runner.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class Region {
	public static boolean equalPosition(Region first, Region second) {
		return first._x1 == second._x1 && first._y1 == second._y1
				&& first._x2 == second._x2 && first._y2 == second._y2;
	}

	public static void main(String[] args) {
		JoinMatrix joinMatrix = new UJMPAdapterByteMatrix(100, 100);
		joinMatrix.setElement(1, 12, 15);
		joinMatrix.setElement(1, 17, 30);
		joinMatrix.setElement(1, 17, 29);
		joinMatrix.setElement(1, 16, 29);
		joinMatrix.setElement(1, 15, 10);

		// WeightPrecomputation wf = new DenseMonotonicWeightPrecomputation(new
		// WeightFunction(1, 1), joinMatrix, new HashMap());
		WeightPrecomputation wf = new DenseWeightPrecomputation(
				new WeightFunction(1, 1), joinMatrix);

		Region region = new Region(1, 3, 50, 60);
		region.minimizeToNotEmpty(wf);
		System.out.println(region);
	}

	public static String toString(List<Region> regions,
			OptimalityMetricInterface opt, String prefix) {
		StringBuilder sb = new StringBuilder();
		double minWeight = Double.MAX_VALUE, maxWeight = 0;
		sb.append("\nThe number of regions is ").append(regions.size());
		for (Region region : regions) {
			double regionWeight = opt.getWeight(region);
			if (regionWeight < minWeight) {
				minWeight = regionWeight;
			}
			if (regionWeight > maxWeight) {
				maxWeight = regionWeight;
			}
			sb.append("\nRegion is ").append(region);
			sb.append(", weight = ").append(regionWeight).append("\n");
		}
		sb.append("\n")
				.append(prefix)
				.append(" region weights are in range [" + minWeight + ", "
						+ maxWeight + "]\n");
		return sb.toString();
	}

	public static String toString(List<Region> regions, String prefix) {
		StringBuilder sb = new StringBuilder();
		sb.append("\nThe number of regions is ").append(regions.size());
		for (Region region : regions) {
			sb.append("\nRegion is ").append(region).append("\n");
		}
		return sb.toString();
	}

	// region is defined with two opposite edge points
	private int _x1, _y1, _x2, _y2;

	private int _frequency;

	/*
	 * All boundaries are inclusive
	 */
	public Region(int x1, int y1, int x2, int y2) {
		_x1 = x1;
		_y1 = y1;
		_x2 = x2;
		_y2 = y2;
	}

	public Region(int x1, int y1, int x2, int y2, int frequency) {
		this(x1, y1, x2, y2);
		_frequency = frequency;
	}

	public Region(Point origUpperLeft, Point origLowerRight) {
		this(origUpperLeft.get_x(), origUpperLeft.get_y(), origLowerRight
				.get_x(), origLowerRight.get_y());
	}

	public Region(Region r) {
		this(r._x1, r._y1, r._x2, r._y2, r._frequency);
	}

	public Region(String regionHash) {
		final String[] splits = regionHash.split("-");
		_x1 = Integer.parseInt(new String(splits[0]));
		_y1 = Integer.parseInt(new String(splits[1]));
		_x2 = Integer.parseInt(new String(splits[2]));
		_y2 = Integer.parseInt(new String(splits[3]));
	}

	private void eliminateEmptyBottomExpGrowth(WeightPrecomputation wp,
			ShallowCoarsener coarsener) {
		int upperBound = _x2;
		int delta = -1;
		int currentPos = upperBound;
		Region region = new Region(currentPos, _y1, _x2, _y2);
		if (!SystemParameters.COARSE_PRECOMPUTATION) {
			region = translateCoarsenedToOriginalRegion(region, coarsener);
		}
		while (currentPos >= _x1 && wp.isEmpty(region)) {
			upperBound = currentPos;
			currentPos += delta;
			delta *= 2;
			if (!(currentPos >= _x1)) {
				break;
			}
			region = new Region(currentPos, _y1, _x2, _y2);
			if (!SystemParameters.COARSE_PRECOMPUTATION) {
				region = translateCoarsenedToOriginalRegion(region, coarsener);
			}
		}
		int lowerBound = currentPos;
		if (lowerBound < _x1) {
			lowerBound = _x1;
		}
		eliminateEmptyBottomRange(lowerBound, upperBound, wp, coarsener);
	}

	private void eliminateEmptyBottomRange(int lowerBound, int upperBound,
			WeightPrecomputation wp, ShallowCoarsener coarsener) {
		int new_x2 = upperBound;
		while (lowerBound <= upperBound) {
			int currentPos = (lowerBound + upperBound) / 2;
			Region region = new Region(currentPos, _y1, _x2, _y2);
			if (!SystemParameters.COARSE_PRECOMPUTATION) {
				region = translateCoarsenedToOriginalRegion(region, coarsener);
			}
			if (!wp.isEmpty(region)) {
				lowerBound = currentPos + 1;
			} else {
				upperBound = currentPos - 1;
				new_x2 = upperBound;
			}
		}
		_x2 = new_x2;
	}

	private void eliminateEmptyLeftExpGrowth(WeightPrecomputation wp,
			ShallowCoarsener coarsener) {
		int lowerBound = _y1;
		int delta = 1;
		int currentPos = lowerBound;
		Region region = new Region(_x1, _y1, _x2, currentPos);
		if (!SystemParameters.COARSE_PRECOMPUTATION) {
			region = translateCoarsenedToOriginalRegion(region, coarsener);
		}
		while (currentPos <= _y2 && wp.isEmpty(region)) {
			lowerBound = currentPos;
			currentPos += delta;
			delta *= 2;
			if (!(currentPos <= _y2)) {
				break;
			}
			region = new Region(_x1, _y1, _x2, currentPos);
			if (!SystemParameters.COARSE_PRECOMPUTATION) {
				region = translateCoarsenedToOriginalRegion(region, coarsener);
			}
		}
		int upperBound = currentPos;
		if (upperBound > _y2) {
			upperBound = _y2;
		}
		eliminateEmptyLeftRange(lowerBound, upperBound, wp, coarsener);
	}

	private void eliminateEmptyLeftRange(int lowerBound, int upperBound,
			WeightPrecomputation wp, ShallowCoarsener coarsener) {
		int new_y1 = lowerBound;
		while (lowerBound <= upperBound) {
			int currentPos = (lowerBound + upperBound) / 2;
			Region region = new Region(_x1, _y1, _x2, currentPos);
			if (!SystemParameters.COARSE_PRECOMPUTATION) {
				region = translateCoarsenedToOriginalRegion(region, coarsener);
			}
			if (!wp.isEmpty(region)) {
				upperBound = currentPos - 1;
			} else {
				lowerBound = currentPos + 1;
				new_y1 = lowerBound;
			}
		}
		_y1 = new_y1;
	}

	private void eliminateEmptyRightExpGrowth(WeightPrecomputation wp,
			ShallowCoarsener coarsener) {
		int upperBound = _y2;
		int delta = -1;
		int currentPos = upperBound;
		Region region = new Region(_x1, currentPos, _x2, _y2);
		if (!SystemParameters.COARSE_PRECOMPUTATION) {
			region = translateCoarsenedToOriginalRegion(region, coarsener);
		}
		while (currentPos >= _y1 && wp.isEmpty(region)) {
			upperBound = currentPos;
			currentPos += delta;
			delta *= 2;
			if (!(currentPos >= _y1)) {
				break;
			}
			region = new Region(_x1, currentPos, _x2, _y2);
			if (!SystemParameters.COARSE_PRECOMPUTATION) {
				region = translateCoarsenedToOriginalRegion(region, coarsener);
			}
		}
		int lowerBound = currentPos;
		if (lowerBound < _y1) {
			lowerBound = _y1;
		}
		eliminateEmptyRightRange(lowerBound, upperBound, wp, coarsener);
	}

	private void eliminateEmptyRightRange(int lowerBound, int upperBound,
			WeightPrecomputation wp, ShallowCoarsener coarsener) {
		int new_y2 = upperBound;
		while (lowerBound <= upperBound) {
			int currentPos = (lowerBound + upperBound) / 2;
			Region region = new Region(_x1, currentPos, _x2, _y2);
			if (!SystemParameters.COARSE_PRECOMPUTATION) {
				region = translateCoarsenedToOriginalRegion(region, coarsener);
			}
			if (!wp.isEmpty(region)) {
				lowerBound = currentPos + 1;
			} else {
				upperBound = currentPos - 1;
				new_y2 = upperBound;
			}
		}
		_y2 = new_y2;
	}

	/*
	 * These calls are more efficient -||-BinarySearch (e.g.
	 * eliminateNonCandidatesTopBinarySearch)
	 */
	private void eliminateEmptyTopExpGrowth(WeightPrecomputation wp,
			ShallowCoarsener coarsener) {
		int lowerBound = _x1;
		int delta = 1;
		int currentPos = lowerBound;
		Region region = new Region(_x1, _y1, currentPos, _y2);
		if (!SystemParameters.COARSE_PRECOMPUTATION) {
			region = translateCoarsenedToOriginalRegion(region, coarsener);
		}
		while (currentPos <= _x2 && wp.isEmpty(region)) {
			lowerBound = currentPos;
			currentPos += delta;
			delta *= 2;
			if (!(currentPos <= _x2)) {
				break;
			}
			region = new Region(_x1, _y1, currentPos, _y2);
			if (!SystemParameters.COARSE_PRECOMPUTATION) {
				region = translateCoarsenedToOriginalRegion(region, coarsener);
			}
		}
		int upperBound = currentPos;
		if (upperBound > _x2) {
			upperBound = _x2;
		}
		eliminateEmptyTopRange(lowerBound, upperBound, wp, coarsener);
	}

	/*
	 * Search only in range
	 */
	private void eliminateEmptyTopRange(int lowerBound, int upperBound,
			WeightPrecomputation wp, ShallowCoarsener coarsener) {
		int new_x1 = lowerBound;
		while (lowerBound <= upperBound) {
			int currentPos = (lowerBound + upperBound) / 2;
			Region region = new Region(_x1, _y1, currentPos, _y2);
			if (!SystemParameters.COARSE_PRECOMPUTATION) {
				region = translateCoarsenedToOriginalRegion(region, coarsener);
			}
			if (!wp.isEmpty(region)) {
				upperBound = currentPos - 1;
			} else {
				lowerBound = currentPos + 1;
				new_x1 = lowerBound;
			}
		}
		_x1 = new_x1;
	}

	public int get_x1() {
		return _x1;
	}

	public int get_x2() {
		return _x2;
	}

	public int get_y1() {
		return _y1;
	}

	public int get_y2() {
		return _y2;
	}

	/*
	 * These points are not part of the Region, but are generated when needed
	 * Corners are organized as here: 01 23
	 */
	public Point getCorner(int cornerNum) {
		switch (cornerNum) {
		case 0:
			return new Point(_x1, _y1);
		case 1:
			return new Point(_x1, _y2);
		case 2:
			return new Point(_x2, _y1);
		case 3:
			return new Point(_x2, _y2);
		default:
			throw new RuntimeException("Corner number " + cornerNum
					+ " does not exist in a rectangle. Allowed are 0, 1, 2, 3.");
		}
	}

	public int getFrequency() {
		return _frequency;
	}

	public int getHalfPerimeter() {
		return getSizeX() + getSizeY();
	}

	// unique hash string per region
	public String getHashString() {
		return _x1 + "-" + _y1 + "-" + _x2 + "-" + _y2;
	}

	/*
	 * all the boundaries are inclusive
	 */
	public int getSizeX() {
		return (_x2 - _x1 + 1);
	}

	/*
	 * all the boundaries are inclusive
	 */
	public int getSizeY() {
		return (_y2 - _y1 + 1);
	}

	/*
	 * Could be done more efficiently for enclosing scc regions, but as we
	 * empirically found out that the majority of regions do not move their
	 * bounds much no need for this optimization
	 */
	public void minimizeToNotEmpty(WeightPrecomputation wp) {
		if (wp.isEmpty(this)) {
			throw new RuntimeException(
					"Should not try to minimize a region with no candidate cells! \nRegion is "
							+ toString());
		}

		// better when regions are mostly filled
		eliminateEmptyTopExpGrowth(wp, null);
		eliminateEmptyLeftExpGrowth(wp, null);
		eliminateEmptyBottomExpGrowth(wp, null);
		eliminateEmptyRightExpGrowth(wp, null);

		/*
		 * // better when regions are mostly empty
		 * eliminateEmptyTopBinarySearch(wp);
		 * eliminateEmptyLeftBinarySearch(wp);
		 * eliminateEmptyBottomBinarySearch(wp);
		 * eliminateEmptyRightBinarySearch(wp);
		 */
	}

	// ******************************************************************************************************

	// ******************************************************************************************************
	public void minimizeToNotEmptyCoarsened(WeightPrecomputation wp,
			ShallowCoarsener coarsener) {
		Region region = this;
		if (!SystemParameters.COARSE_PRECOMPUTATION) {
			region = coarsener.translateCoarsenedToOriginalRegion(region);
		}
		if (wp.isEmpty(region)) {
			throw new RuntimeException(
					"Should not try to minimize a region with no candidate cells! \nRegion is "
							+ toString());
		}

		// better when regions are mostly filled
		eliminateEmptyTopExpGrowth(wp, coarsener);
		eliminateEmptyLeftExpGrowth(wp, coarsener);
		eliminateEmptyBottomExpGrowth(wp, coarsener);
		eliminateEmptyRightExpGrowth(wp, coarsener);
	}

	public void set_x1(int x1) {
		_x1 = x1;
	}

	public void set_x2(int x2) {
		_x2 = x2;
	}

	public void set_y1(int y1) {
		_y1 = y1;
	}

	public void set_y2(int y2) {
		_y2 = y2;
	}

	public void setFrequency(int frequency) {
		_frequency = frequency;
	}

	/*
	 * Corner cornerNum is exchanged with newCorner
	 */
	public Region shiftCorner(int cornerNum, Point newCorner) {
		int x1 = -1, y1 = -1, x2 = -1, y2 = -1;

		switch (cornerNum) {
		case 0:
			x1 = newCorner.get_x();
			y1 = newCorner.get_y();
			break;
		case 1:
			x1 = newCorner.get_x();
			y2 = newCorner.get_y();
			break;
		case 2:
			x2 = newCorner.get_x();
			y1 = newCorner.get_y();
			break;
		case 3:
			x2 = newCorner.get_x();
			y2 = newCorner.get_y();
			break;
		default:
			throw new RuntimeException("Corner number " + cornerNum
					+ " does not exist in a rectangle. Allowed are 0, 1, 2, 3.");
		}
		return new Region(x1, y1, x2, y2);
	}

	@Override
	public String toString() {
		return "Upper Left corner is (" + _x1 + ", " + _y1
				+ "), Lower Right corner is (" + _x2 + ", " + _y2 + ")";
	}

	private Region translateCoarsenedToOriginalRegion(Region region,
			ShallowCoarsener coarsener) {
		if (coarsener != null) {
			// if we are here, the coordinates of this region are coarsened;
			// we need to invoke wp method on the original points
			return coarsener.translateCoarsenedToOriginalRegion(region);
		} else {
			return region;
		}
	}

	/*
	 * public void minimizeToNonEmpty(int cornerNum, DenseWeightPrecomputation
	 * wp) { if (wp.isEmpty(this)){ throw new RuntimeException(
	 * "Should not try to minimize a region with no candidate cells!"); }
	 * switch(cornerNum){ case 0: eliminateEmptyTopExpGrowth(wp);
	 * eliminateEmptyLeftExpGrowth(wp); break; case 1:
	 * eliminateEmptyTopExpGrowth(wp); eliminateEmptyRightExpGrowth(wp); break;
	 * case 2: eliminateEmptyBottomExpGrowth(wp);
	 * eliminateEmptyLeftExpGrowth(wp); break; case 3:
	 * eliminateEmptyBottomExpGrowth(wp); eliminateEmptyRightExpGrowth(wp);
	 * break; default: throw new RuntimeException("Corner number " + cornerNum +
	 * " does not exist in a rectangle. Allowed are 0, 1, 2, 3."); } }
	 */

	/*
	 * These calls are less efficient (unless region is almost completely empty)
	 */
	/*
	 * private void eliminateEmptyTopBinarySearch(WeightPrecomputation wp){ int
	 * lowerBound = _x1; int upperBound = _x2;
	 * eliminateEmptyTopRange(lowerBound, upperBound, wp); }
	 * 
	 * private void eliminateEmptyBottomBinarySearch(WeightPrecomputation wp){
	 * int lowerBound = _x1; int upperBound = _x2;
	 * eliminateEmptyBottomRange(lowerBound, upperBound, wp); }
	 * 
	 * private void eliminateEmptyLeftBinarySearch(WeightPrecomputation wp){ int
	 * lowerBound = _y1; int upperBound = _y2;
	 * eliminateEmptyLeftRange(lowerBound, upperBound, wp); }
	 * 
	 * private void eliminateEmptyRightBinarySearch(WeightPrecomputation wp){
	 * int lowerBound = _y1; int upperBound = _y2;
	 * eliminateEmptyRightRange(lowerBound, upperBound, wp); }
	 */
}