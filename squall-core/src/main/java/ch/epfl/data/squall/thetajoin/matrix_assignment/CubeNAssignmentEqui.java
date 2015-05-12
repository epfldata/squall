package ch.epfl.data.squall.thetajoin.matrix_assignment;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

/**
 * Apply the idea of equal-size matrix partitioning of the paper "Scalable and Adaptive Online Joins" for hypercube.<br/>
 * http://wiki.epfl.ch/bigdata2015-hypercubejoins/hcp-equal-size <br/>
 * Basically, it divides the join hypercube into equal-size regions by solving the two equations:<br/>
 * rd[0] * ... * rd[k-1] = r <br/>
 * sizes[0] / rd[0] = sizes[1] / rd[1] = ... = sizes[k] / rd[k] <br/>
 * 
 * @author Tam
 * @param <KeyType>
 */
public class CubeNAssignmentEqui<KeyType> implements Serializable, HyperCubeAssignment<KeyType> {

	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(CubeNAssignmentEqui.class);

	private Random rand;
	private int[] _rd;
	private final int _r;
	private long[] sizes;
	private Map<String, Integer> regionIDsMap;
	private Comparator<Assignment> _comparator = new CombineCost();
	
	// Only for testing small join cube, as the cache is equal to the size of join cube (=sizes[0] * ... * sizes[k-1])
	private boolean useRegionMapCache = false; 

	public CubeNAssignmentEqui(long[] sizes, int r, long randomSeed) {
		rand = randomSeed == -1 ? new Random() : new Random(randomSeed);

		this.sizes = sizes;
		this._rd = new int[sizes.length];
		this._r = r;
		compute();
		if (useRegionMapCache) createRegionMap(); 
	}
	
	public CubeNAssignmentEqui(long[] sizes, int r, long randomSeed, Comparator<Assignment> comparator) {
		rand = randomSeed == -1 ? new Random() : new Random(randomSeed);

		this.sizes = sizes;
		this._rd = new int[sizes.length];
		this._r = r;
		this._comparator = comparator;
		compute();
		if (useRegionMapCache) createRegionMap(); 
	}

	private void compute() {
		long compare = _r;
		for (long size : sizes) {
			compare = compare / size;
		}
		
		// If #joiners larger than the size of join matrix itself, each cell is a partition
		if (compare >= 1) {
			for (int i = 0; i < _rd.length; i++) {
				_rd[i] = (int) sizes[i];
			}
			return;
		}

		// Solve the two equivalent partitioning equations
		double[] RdExactValues = new double[_rd.length];
		for (int i = 0; i < _rd.length; i++) {
			double rootNOfRatios = Math.pow(_r, 1.0 / _rd.length);
			for (int j = 0; j < _rd.length; j++){
				double ratio = (double) sizes[i] / sizes[j];
				rootNOfRatios *= Math.pow(ratio, 1.0 / _rd.length);
			}
			RdExactValues[i] = rootNOfRatios;
			
		}
		
		// Round up the real values less than 1 (e.g. 0.78 -> 1) by reducing the other maximal values
		while (Utilities.existsLess(RdExactValues, 1.0)) {
			for (int i = 0; i < RdExactValues.length; i++){
				if (RdExactValues[i] < 1) {
					int j = Utilities.indexOfMax(RdExactValues);
					RdExactValues[j] *= RdExactValues[i];
					RdExactValues[i] = 1;
				}
			}
		}
		
		// Adjust the real values to integer values
		int[][] possibleValuesOfRd = new int[_rd.length][2];
		for (int i = 0; i < _rd.length; i++) {
			possibleValuesOfRd[i][0] = (int) Math.floor(RdExactValues[i]);
			possibleValuesOfRd[i][1] = (int) Math.ceil(RdExactValues[i]);
		}
		
		// We find the best partition as hypercubes
		int[] rd = new int[_rd.length];
		Arrays.fill(rd, 0);
		
		ArrangementIterator binaryGenerator = new ArrangementIterator(Arrays.asList(0,1), rd.length);
		int count = 0;
		while (binaryGenerator.hasNext()) {
			List<Integer> combination = binaryGenerator.next();
			
			for (int dim = 0; dim < rd.length; dim++) {
				rd[dim] = possibleValuesOfRd[dim][combination.get(dim)];
			}

			int r = Utilities.multiply(rd);
			if (r < _r * 0.5 || r > _r) continue; // Only tolerate from 100% to 50% of the machines (e.g 1999 -> 1000)
			
			if (count == 0) {
				Utilities.copy(rd, _rd);
			} else {
				
				// If new assignment is better than the best assignment so far
				if (_comparator.compare(new Assignment(sizes, rd), new Assignment(sizes, _rd)) > 0){
					Utilities.copy(rd, _rd);
				}
			}
			count++;
		}

		assert count > 0: "Not found any assignment satisfying the equations";
	}

	private void createRegionMap() {
		regionIDsMap = new HashMap<String, Integer>();
		CellIterator gen = new CellIterator(_rd);
		while (gen.hasNext()) {
			List<Integer> cellIndex = gen.next();
			mapRegionID(cellIndex);
		}
	}

	private int mapRegionID(List<Integer> regionIndex) {
		assert _rd.length == regionIndex.size();

		// Look up at cache first
		if (useRegionMapCache) {
			assert regionIDsMap.containsKey(getMappingIndexes(regionIndex));
			return regionIDsMap.get(getMappingIndexes(regionIndex));
		}

		// Compute if not found in cache
		int regionID = 0;
		for (int i = regionIndex.size() - 1; i >= 0; i--) {
			int dimAmount = regionIndex.get(i);
			for (int dim = _rd.length - 1; dim > i; dim--) {
				dimAmount *= _rd[dim];
			}
			regionID += dimAmount;
		}

		return regionID;
	}
	
	@Override
	public List<Integer> getRegionIDs(Dimension dim) {
		final List<Integer> regionIDs = new ArrayList<Integer>();

		if (dim.val() >= 0 && dim.val() < sizes.length) {
			final int randomIndex = rand.nextInt(_rd[dim.val()]);
			CellIterator gen = new CellIterator(_rd, dim.val(), randomIndex);
			while (gen.hasNext()) {
				List<Integer> cellIndex = gen.next();
				int regionID = mapRegionID(cellIndex);
				regionIDs.add(regionID);
			}
			assert regionIDs.size() == Utilities.multiply(_rd) / _rd[dim.val()];
		} else {
			LOG.info("ERROR not a possible index assignment.");
		}

		return regionIDs;
	}

	@Override
	public List<Integer> getRegionIDs(Dimension dim, KeyType key) {
		throw new RuntimeException("This method is content-insenstive");
	}

	@Override
	public String toString() {
		return getMappingDimensions();
	}

	@Override
	public String getMappingDimensions() {
		StringBuilder sb = new StringBuilder();
		String prefix = "";
		for (int r : _rd) {
			sb.append(prefix);
			prefix = "-";
			sb.append(r);
		}
		return sb.toString();
	}

	public static String getMappingIndexes(List<Integer> regionIndex) {
		StringBuilder sb = new StringBuilder();
		String prefix = "";
		for (Integer r : regionIndex) {
			sb.append(prefix);
			prefix = "-";
			sb.append(r);
		}
		return sb.toString();
	}

	@Override
	public int getNumberOfRegions(Dimension dim) {
		if (dim.val() >= 0 && dim.val() < _rd.length) {
			return _rd[dim.val()];
		} else {
			throw new RuntimeException("Dimension is invalid");
		}
	}
	
	/**
	 * The actual number of regions with the best prime factorization.
	 */
	public int getNumberOfRegions(){
		return Utilities.multiply(_rd);
	}

	public static void main(String[] args) {
//		testcase1();
//		testcase2();
//		testcase3();
		testcase4();
	}
	
	public static void testcase1(){
		List<CubeNAssignmentEqui> tests = Arrays.asList(
				new CubeNAssignmentEqui(Utilities.arrayOf(13, 7), 1, -1), new CubeNAssignmentEqui(Utilities.arrayOf(4, 4, 4), 8, -1),
				new CubeNAssignmentEqui(Utilities.arrayOf(4, 4, 4, 4), 16, -1), new CubeNAssignmentEqui(Utilities.arrayOf(8, 4, 10, 7), 1000, -1), 
				new CubeNAssignmentEqui(Utilities.arrayOf(10, 10, 10, 10), 1021, -1)
				);
		for (CubeNAssignmentEqui test : tests) {
			LOG.info("Input: " + Arrays.toString(test.sizes) + ", " + test._r);
			LOG.info("#Reducers each dimension: " + test.toString());
			for (int i = 0; i < 3; i++) {
				LOG.info("Get Regions of dimension 1: " + test.getRegionIDs(Dimension.d(0)).toString());
			}
		}
	}
	
	public static void testcase2(){
		List<CubeNAssignmentEqui> tests = Arrays.asList(
				new CubeNAssignmentEqui(Utilities.arrayOf(10000, 1000,100,100,100,100,100,100), 1000, -1)
				);
		for (CubeNAssignmentEqui test : tests) {
			LOG.info("Input: " + Arrays.toString(test.sizes) + ", " + test._r);
			LOG.info("#Reducers each dimension: " + test.toString());
			for (int i = 0; i < 3; i++) {
				LOG.info("Get Regions of dimension 1: " + test.getRegionIDs(Dimension.d(0)).toString());
			}
		}
	}
	
	public static void testcase3(){
		List<CubeNAssignmentEqui> tests = Arrays.asList(
				new CubeNAssignmentEqui(Utilities.arrayOf(10000, 10000, 100,100,100,100,100,100), 1000, -1)
				);
		for (CubeNAssignmentEqui test : tests) {
			LOG.info("Input: " + Arrays.toString(test.sizes) + ", " + test._r);
			LOG.info("#Reducers each dimension: " + test.toString());
			for (int i = 0; i < 3; i++) {
				LOG.info("Get Regions of dimension 1: " + test.getRegionIDs(Dimension.d(0)).toString());
			}
		}
	}
	
	public static void testcase4(){
		List<CubeNAssignmentEqui> tests = Arrays.asList(
				new CubeNAssignmentEqui(Utilities.arrayOf(10000, 10000, 10000, 10000, 10000, 10000, 100, 100), 1000, -1)
				);
		for (CubeNAssignmentEqui test : tests) {
			LOG.info("Input: " + Arrays.toString(test.sizes) + ", " + test._r);
			LOG.info("#Reducers each dimension: " + test.toString());
			for (int i = 0; i < 3; i++) {
				LOG.info("Get Regions of dimension 1: " + test.getRegionIDs(Dimension.d(0)).toString());
			}
		}
	}
	
}
