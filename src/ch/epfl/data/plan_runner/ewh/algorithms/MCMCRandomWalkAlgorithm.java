package ch.epfl.data.plan_runner.ewh.algorithms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import ch.epfl.data.plan_runner.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.Point;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

/*
 * Markov chain Monte carlo Random Walk
 */
public class MCMCRandomWalkAlgorithm implements TilingAlgorithm {
	private static class Goodness {
		// direct
		private JoinMatrix _joinMatrix;
		private List<Region> _regions;

		// derived
		private double _maxWeight;
		private double _avgWeight;
		private double _lst;
		private double _avgRegionDensity; // derived from weight and perimeter

		public Goodness(JoinMatrix joinMatrix, List<Region> regions,
				WeightPrecomputation wf) {
			_joinMatrix = joinMatrix;
			_regions = regions;

			// compute others
			Region maxRegion = Collections.max(_regions,
					new RegionWeightComparator(wf));
			_maxWeight = wf.getWeight(maxRegion);
			_avgWeight = getAvgWeight(_regions, wf);
			_lst = getLstWeight(_regions, wf);
			_avgRegionDensity = getAvgRegionDensity(_regions, wf);
		}

		/*
		 * bigger than 1 means that 'this' is better than 'other'
		 */
		public double compare(Goodness other) {
			if (_maxWeight != other._maxWeight) {
				return 1 / (_maxWeight / other._maxWeight);
			} else if (_lst != other._lst) {
				return 1 / (_lst / other._lst);
			} else {
				return 1 / (_avgRegionDensity / other._avgRegionDensity);
			}
		}
	}

	public static class RegionWeightComparator implements Comparator<Region> {
		private WeightPrecomputation _wf;

		public RegionWeightComparator(WeightPrecomputation wf) {
			_wf = wf;
		}

		@Override
		public int compare(Region region1, Region region2) {
			double w1 = _wf.getWeight(region1);
			double w2 = _wf.getWeight(region2);
			if (w1 > w2)
				return -1;
			if (w1 == w1)
				return 0;
			return 1;
		}
	}

	public static class RegionXComparator implements Comparator<Region> {
		@Override
		public int compare(Region region1, Region region2) {
			int x11 = region1.get_x1();
			int x21 = region2.get_x1();
			if (x11 > x21)
				return -1;
			if (x11 == x21)
				return 0;
			return 1;
		}
	}

	public static class RegionYComparator implements Comparator<Region> {
		@Override
		public int compare(Region region1, Region region2) {
			int y11 = region1.get_y1();
			int y21 = region2.get_y1();
			if (y11 > y21)
				return -1;
			if (y11 == y21)
				return 0;
			return 1;
		}
	}

	private static class SpatialRegions {
		List<Region> _regions;
		List<Region> _sortedRegionsX;
		List<Region> _sortedRegionsY;

		public SpatialRegions(List<Region> regions) {
			_regions = new ArrayList<Region>();
			_regions.addAll(regions);
			_sortedRegionsX = new ArrayList<Region>();
			_sortedRegionsX.addAll(regions);
			_sortedRegionsY = new ArrayList<Region>();
			_sortedRegionsY.addAll(regions);

			Collections.sort(_sortedRegionsX, new RegionXComparator());
			Collections.sort(_sortedRegionsY, new RegionYComparator());
		}

		/*
		 * TODO find existing intersection by going through sorted lists
		 */
		public boolean isIntersect(Region region) {
			return false;
		}
	}

	public static class TwoBoolean {
		private boolean _first;
		private boolean _second;

		public TwoBoolean(boolean first, boolean second) {
			_first = first;
			_second = second;
		}

		public boolean getFirst() {
			return _first;
		}

		public boolean getSecond() {
			return _second;
		}
	}

	/*
	 * shiftedRegion is a separate copy of a modification of
	 * regions.get(shiftRegionNum) Adjusting: randomly pick according to the
	 * leftover proportion the smallest increase from neighbors smallest total
	 * number of recursions
	 * 
	 * You also have to take candidate cells into account
	 */
	private static List<Region> adjustRegions(List<Region> regions,
			int shiftedRegionNum, Region shiftedRegion) {
		// TODO priority
		return null;
	}

	/*
	 * We opt for a region further away from avgWeight with higher probability
	 */
	private static int chooseRegion(List<Region> regions, double avgWeight) {
		// TODO Auto-generated method stub
		return 0;
	}

	private static double getAvgRegionDensity(List<Region> regions,
			WeightPrecomputation wf) {
		double totalDensity = 0;
		for (Region region : regions) {
			double weight = wf.getWeight(region);
			int halfPerimeter = region.getHalfPerimeter();
			double density = weight / halfPerimeter;
			totalDensity += density;
		}
		return totalDensity / regions.size();
	}

	private static double getAvgWeight(List<Region> regions,
			WeightPrecomputation wf) {
		double totalWeight = 0;
		for (Region region : regions) {
			totalWeight += wf.getWeight(region);
		}
		return totalWeight / regions.size();
	}

	private static double getLstWeight(List<Region> regions,
			WeightPrecomputation wf) {
		double lst = 0;
		for (Region region : regions) {
			double weight = wf.getWeight(region);
			lst += weight * weight;
		}
		return Math.sqrt(lst);
	}

	/*
	 * Initial regions covers all the 1-elements by using a simple algorithm /
	 * division on rows for example
	 */
	private static List<Region> initializeRegions(JoinMatrix joinMatrix) {
		// TODO Auto-generated method stub
		return null;
	}

	private Map _map;

	private int _j;

	private Random _randomGen = new Random();

	private static final double THRESHOLD_PROB = 0.8; // 80%

	private static final double MIN_SHIFT_PORTION = 0.01;

	private static final int MAX_GAUSS_RANDOM = 4;

	public MCMCRandomWalkAlgorithm(Map map, int j) {
		_map = map;
		_j = j;
	}

	// the output of this Gaussian is from [-MAX_GAUSS_RANDOM,
	// MAX_GAUSS_RANDOM], where the MAX_GAUSS_RANDOM = 4, in more than 99.9% of
	// cases
	private int generateShift(int dimSize, boolean toReduce) {
		// boundaries for the shift
		int minShift = (int) (MIN_SHIFT_PORTION * dimSize / _j);
		int maxShift = dimSize / _j;

		double random = Math.abs(_randomGen.nextGaussian());
		int shift = (int) (minShift + random / MAX_GAUSS_RANDOM
				* (maxShift - minShift));
		if (shift > maxShift) {
			// just in case that we generated a value higher than
			// MAX_GAUSS_RANDOM
			shift = maxShift;
		}
		if (toReduce) {
			shift = -shift;
		}
		return shift;
	}

	public TwoBoolean getChangeDirection(int cornerNum, boolean isSmallerWeight) {
		// each coordinate has bigger chance either to reduce or to increase
		boolean reduceX = false;
		boolean reduceY = false;

		// if isSmaller
		if (cornerNum < 2) {
			reduceX = true;
		}
		if (cornerNum % 2 == 0) {
			reduceY = true;
		}

		if (!isSmallerWeight) {
			// isBigger is an opposite
			reduceX = !reduceX;
			reduceY = !reduceY;
		}

		// with higher probability do what was suggested; otherwise take an
		// opposite step
		double shiftProb = _randomGen.nextDouble();
		if (shiftProb >= THRESHOLD_PROB) {
			reduceX = !reduceX;
			reduceY = !reduceY;
		}

		return new TwoBoolean(reduceX, reduceY);
	}

	@Override
	public WeightPrecomputation getPrecomputation() {
		throw new RuntimeException("Implement me please!");
	}

	@Override
	public String getShortName() {
		return "mcmc";
	}

	@Override
	public double getWeight(Region region) {
		throw new RuntimeException("Implement me!");
	}

	@Override
	public WeightFunction getWeightFunction() {
		throw new RuntimeException("Implement me!");
	}

	/*
	 * If alpha > 1, return true; otherwise return true with probability alpha
	 */
	private boolean isAccept(double alpha) {
		if (alpha >= 1) {
			return true;
		} else {
			double rndNumber = _randomGen.nextDouble();
			return (rndNumber > alpha);
		}
		// TODO simulated annealing
	}

	@Override
	public List<Region> partition(JoinMatrix joinMatrix, StringBuilder sb) {
		WeightPrecomputation wp = null;
		if (SystemParameters.MONOTONIC_PRECOMPUTATION) {
			wp = new DenseMonotonicWeightPrecomputation(
					new WeightFunction(1, 1), joinMatrix, _map); // TODO make
			// them
			// parameters
		} else {
			wp = new DenseWeightPrecomputation(new WeightFunction(1, 1),
					joinMatrix); // TODO make them parameters
		}
		List<Region> previousRegions = initializeRegions(joinMatrix);
		Goodness previousGoodness = new Goodness(joinMatrix, previousRegions,
				wp);
		List<Region> bestRegions = previousRegions;
		Goodness bestGoodness = previousGoodness;

		for (int i = 0; i < 10; i++) {
			double avgWeight = getAvgWeight(previousRegions, wp);
			int shiftedRegionNum = chooseRegion(previousRegions, avgWeight);
			Region choosenRegion = previousRegions.get(shiftedRegionNum);
			boolean isSmallerWeight = wp.getWeight(choosenRegion) < avgWeight;
			Region shiftedRegion = randomShift(choosenRegion, wp,
					isSmallerWeight, joinMatrix.getXSize(),
					joinMatrix.getYSize());
			List<Region> currentRegions = adjustRegions(previousRegions,
					shiftedRegionNum, shiftedRegion);
			Goodness currentGoodness = new Goodness(joinMatrix, currentRegions,
					wp);
			double alpha = currentGoodness.compare(previousGoodness);
			if (alpha > 1 && currentGoodness.compare(bestGoodness) > 1) {
				// we are better than previous mapping, and better than the
				// previously known best mapping
				bestRegions = currentRegions;
				bestGoodness = currentGoodness;
			}
			boolean accept = isAccept(alpha);
			if (accept) {
				previousRegions = currentRegions;
				previousGoodness = new Goodness(joinMatrix, previousRegions, wp);
			}
		}

		return bestRegions;
	}

	/*
	 * If the point is outside of the matrix, move it inside
	 */
	private Point putWithinMatrix(Point corner, int xSize, int ySize) {
		int shiftedX = putWithinMatrixDim(corner.get_x(), xSize);
		int shiftedY = putWithinMatrixDim(corner.get_y(), ySize);
		return new Point(shiftedX, shiftedY);
	}

	private int putWithinMatrixDim(int position, int size) {
		if (position < 0) {
			position = 0;
		}
		if (position > size - 1) {
			position = size - 1;
		}
		return position;
	}

	/*
	 * Randomly pick one of the four corners If the region is smaller than AVG,
	 * with higher probability choose to change both x,y such that region grows
	 * otherwise, with higher probability choose to change both x,y such that
	 * regions decreases Step size is Gauss 0 (dim/J*100), dim/J
	 * 
	 * Make sure that the step is not outside of the matrix; and that there are
	 * some candidate cells
	 * 
	 * isSmaller - does current region has smaller weight than avgWeight
	 */
	private Region randomShift(Region region, WeightPrecomputation wf,
			boolean isSmallerWeight, int xSize, int ySize) {
		int cornerNum = -1;
		Point cornerToShift = null;
		Point shiftedCorner = null;
		Region result;
		boolean exitCondition = true;

		do {
			cornerNum = _randomGen.nextInt(4);
			cornerToShift = region.getCorner(cornerNum);

			TwoBoolean tb = getChangeDirection(cornerNum, isSmallerWeight);

			// finally, compute shifts
			int shiftX = generateShift(xSize, tb.getFirst());
			int shiftY = generateShift(ySize, tb.getSecond());
			shiftedCorner = cornerToShift.shift(shiftX, shiftY);

			// now check the conditions
			// first, move the point inside the matrix and check if any shift
			// from the original point is made
			shiftedCorner = putWithinMatrix(shiftedCorner, xSize, ySize);
			boolean isCornerShifted = !cornerToShift.equals(shiftedCorner);
			if (!isCornerShifted)
				continue;

			// second, check if the region contains at least one candidate cell
			result = region.shiftCorner(cornerNum, cornerToShift);
			if (wf.isEmpty(result))
				continue;

			// third, minimize the perimeter, give the candidate cells inside
			// the result region
			// TODO comment result = result.minimizeToNonEmpty(cornerNum, wf);
			// TODO Optimization: if increase in size, we could check only the
			// added part of the rectangle for candidates
			break;
		} while (true);

		return result;
	}
}