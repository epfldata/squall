package ch.epfl.data.plan_runner.ewh.algorithms;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.MatrixIntInt;
import ch.epfl.data.plan_runner.ewh.data_structures.Point;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;
import ch.epfl.data.plan_runner.ewh.data_structures.SimpleMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.SparseMatrixUJMP;
import ch.epfl.data.plan_runner.ewh.data_structures.UJMPAdapterByteMatrix;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

/*
 * Answer weight queries for arbitrary regions within the (sample) joinMatrix
 * To reduce complexity to O(1), it uses precomputation
 */
public class DenseMonotonicWeightPrecomputation implements WeightPrecomputation{
	private static Logger LOG = Logger.getLogger(DenseMonotonicWeightPrecomputation.class);
	
	private Map _map;
	
	private WeightFunction _wf;
	
	private JoinMatrix _joinMatrix;
	
	private SimpleMatrix _prefixSum; // dimension of the sample matrix n_s (called here _joinMatrix)
	private int _xSize, _ySize; // dimensions of the prefix sum
	
	private ShallowCoarsener _sc;
	private Map<Integer, Integer> _lastCandCellRow = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> _firstCandCellRow = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> _lastCandCellColumn = new HashMap<Integer, Integer>();
	
	public DenseMonotonicWeightPrecomputation(WeightFunction wf, JoinMatrix joinMatrix, Map map){
		_map = map;
		_wf = wf;
		_joinMatrix = joinMatrix;
		_xSize = _joinMatrix.getXSize();
		_ySize = _joinMatrix.getYSize();
		
		precompute();
	}

	public DenseMonotonicWeightPrecomputation(WeightFunction wf, JoinMatrix joinMatrix, Map map, ShallowCoarsener sc){
		_map = map;
		_wf = wf;
		_joinMatrix = joinMatrix;
		_sc = sc;
		_xSize = _joinMatrix.getXSize();
		_ySize = _joinMatrix.getYSize();
		
		precompute();
	}
	
	@Override
	public WeightFunction getWeightFunction(){
		return _wf;
	}
	
	@Override
	public int getXSize() {
		return _xSize;
	}

	@Override
	public int getYSize() {
		return _ySize;
	}		
	
	/*
	 * this method does *not* use region._frequency, as it is 0
	 */
	@Override
	public double getWeight(Region region){
		return _wf.getWeight(region.getHalfPerimeter(), getFrequency(region));
	}
	
	@Override
	public boolean isEmpty(Region region){
		return getFrequency(region) == 0;
	}
	
	@Override
	public int getFrequency(Region region){
		int corner0x = region.getCorner(0).get_x() - 1;
		int corner0y = region.getCorner(0).get_y() - 1;
		int corner1x = region.getCorner(1).get_x() - 1;
		int corner1y = region.getCorner(1).get_y();
		int corner2x = region.getCorner(2).get_x();
		int corner2y = region.getCorner(2).get_y() - 1;
		int corner3x = region.getCorner(3).get_x(); // this point is inclusive
		int corner3y = region.getCorner(3).get_y();
		
		return getPrefixSum(corner3x, corner3y) - getPrefixSum(corner2x, corner2y) - getPrefixSum(corner1x, corner1y) + getPrefixSum(corner0x, corner0y);
	}

	@Override
	public int getTotalFrequency() {
		return getPrefixSum(_xSize - 1, _ySize - 1);
	}
	
	@Override
	public String toString(){
		return _wf.toString(); 
	}
	 
	private void precompute(){
		StringBuilder sb = new StringBuilder();
		
		if(_sc == null){
			// if not sent through constructor, we build a new InputShallowCoarsener

			// more efficient for PREFIX_SUM_TYPE = INT_INT: only small portion of prefixSum elements are candidates, and thus is assigned a non-zero value
			//int numXBuckets = 100, numYBuckets = 100;
			
			//more efficient for = UJMP
			//int numXBuckets = 4000, numYBuckets = 4000;
			//better to have a higher number of buckets
			int divider = 12;
			if(SystemParameters.isExisting(_map, "PRECOMPUTATION_DIVIDER")){
				divider = SystemParameters.getInt(_map, "PRECOMPUTATION_DIVIDER");
				LOG.info("Setting PRECOMPUTATION_DIVIDER = " + divider);
			}
			int numXBuckets = _xSize/divider, numYBuckets = _ySize/divider;
			
			LOG.info("p_i_x = " + numXBuckets + ", p_i_y = " + numYBuckets);
			
			// ensures that the last bucket is at most 2 * 2 = 4 times bigger than other buckets
			numXBuckets = MyUtilities.adjustPartitioning(_xSize, numXBuckets, "p_i_x");
			numYBuckets = MyUtilities.adjustPartitioning(_ySize, numYBuckets, "p_i_y");
			
			if(_xSize < numXBuckets){
				numXBuckets = _xSize;
			}
			if(_ySize < numYBuckets){
				numYBuckets = _ySize;
			}
			_sc = new InputShallowCoarsener(numXBuckets, numYBuckets);
			_sc.setOriginalMatrix(_joinMatrix, sb);
		}
		
		choosePrefixSumType();

		// computing who is candidate and who is the last candidate per row
		int firstCandInLastLine = 0;
		for(int i = 0; i < _sc.getNumXCoarsenedPoints(); i++){
			boolean isFirstInLine = true;
			 // first and last original points per candidate row
			int rx1 = _sc.getOriginalXCoordinate(i, false);
			int rx2 = _sc.getOriginalXCoordinate(i, true);
			int ry1 = 0, ry2 = 0;
			
			for (int j = firstCandInLastLine; j < _sc.getNumYCoarsenedPoints(); j++){
				int y1 = _sc.getOriginalYCoordinate(j, false);
				int y2 = _sc.getOriginalYCoordinate(j, true);
				Region region = new Region(rx1, y1, rx2, y2);
				boolean isCandidate = MyUtilities.isCandidateRegion(_joinMatrix, region, _joinMatrix.getComparisonPredicate(), _map); 
				if(isCandidate){
					// only candidates should be analyzed for setting to 1
					if(isFirstInLine){
						ry1 = y1;
						_firstCandCellRow.put(i, j);
						firstCandInLastLine = j;
						isFirstInLine = false;
					}
					
					_lastCandCellColumn.put(j, i);
					_lastCandCellRow.put(i, j);
					ry2 = y2;
				}
				if(!isFirstInLine && !isCandidate){
					// I am right from the candidate are; the first non-candidate guy means I should switch to the next row
					break;
				}
			}
			// visit everything at once per candidate coarsened row
			computePrefix(rx1, ry1, rx2, ry2);
		}
		
		if(_prefixSum.getNumElements() > _prefixSum.getCapacity()){
			throw new RuntimeException("Cannot put " + _prefixSum.getNumElements() + " into matrix of capacity " + _prefixSum.getCapacity());
		}
	}
	
	private void choosePrefixSumType() {
		String prefixSumType = "INT_INT"; // this is the default int[][]
		if(SystemParameters.isExisting(_map, "PREFIX_SUM_TYPE")){
			prefixSumType = SystemParameters.getString(_map, "PREFIX_SUM_TYPE");
		}
		
		if(prefixSumType.equalsIgnoreCase("INT_INT")){
			LOG.info("PrefixSumType is int[][]");
			_prefixSum = new MatrixIntInt(_xSize, _ySize);
		}else if(prefixSumType.equalsIgnoreCase("UJMP")){
			int numCandidateRoundedCells = _sc.getNumCandidateRoundedCells(_map);
			// sc is InputShallowCoarsener, so the area is fixed
			int roundedCellArea = (_xSize/_sc.getNumXCoarsenedPoints() + 1) * (_ySize/_sc.getNumYCoarsenedPoints() + 1); // in terms of sample matrix cells
			if(_sc instanceof InputShallowCoarsener){
				// all the cell sizes are equal except the last one which can be bigger
				roundedCellArea *= 2;
			}else{
				// roundedCellArea differs from cell to cell; we multiply with an arbitrary constant
				LOG.info("Should be here only when IIPrecDense or IIPrecBoth");
				roundedCellArea *= 4;
			}
			int capacity = numCandidateRoundedCells * roundedCellArea;
			LOG.info("PrefixSumType is UJMP: Capacity is " + capacity + " = " + numCandidateRoundedCells + " * " + roundedCellArea + " (maybe some multiplier).");
			_prefixSum = new SparseMatrixUJMP(capacity, _xSize, _ySize);
		}else{
			throw new RuntimeException("Unsupported prefixSumType = " + prefixSumType);
		}
	}

	private void computePrefix(int x1, int y1, int x2, int y2){
		int currentRowSum;
		for(int x = x1; x <= x2; x++){
			currentRowSum = 0;
			for (int y = y1; y <= y2; y++){
				currentRowSum += _joinMatrix.getElement(x, y);
				int value = currentRowSum;
				if(x != 0){
					// prefixUp was _prefixSum[x-1][y]
					value += computePrefixUp(x1, x, y);
				}
				_prefixSum.setElement(value, x, y);
			}
		}
	}
	
	// it's not invoked for originalX = 0
	private int computePrefixUp(int originalXBeginBucket, int originalX, int originalY) {
		if(originalX == originalXBeginBucket){
			// this is the first row in the coarsened row
			int coarsenedX = _sc.getCoarsenedXCoordinate(originalX);
			int coarsenedY = _sc.getCoarsenedYCoordinate(originalY);
			int upCoarsenedX = coarsenedX - 1;
			int upCoarsenedY = coarsenedY;
			int lastCandY = _lastCandCellRow.get(upCoarsenedX);
			// Due to monotonicity, cannot be left from the first candidate from the above row
			if(upCoarsenedY > lastCandY){
				// the up is not a candidate; move y left 
				originalY = _sc.getOriginalYCoordinate(lastCandY, true);
			}
		}
		return _prefixSum.getElement(originalX-1, originalY);
	}
	
	@Override
	public int getPrefixSum(int x, int y){
		if(x < 0 || y < 0){
			return 0;
		}else{
			int result = _prefixSum.getElement(x, y); 
			if(result == 0){
				int coarsenedX = _sc.getCoarsenedXCoordinate(x);
				int coarsenedY = _sc.getCoarsenedYCoordinate(y);
				int lastCandY = _lastCandCellRow.get(coarsenedX);
				int firstCandY = _firstCandCellRow.get(coarsenedX);
				if(coarsenedY > lastCandY){
					// right from the last candidate in the row, should ask left
					y = _sc.getOriginalYCoordinate(lastCandY, true);
				}else if (coarsenedY < firstCandY){
					// left from the first candidate in the row, should ask up
					if(_lastCandCellColumn.containsKey(coarsenedY)){
						int lastCandX = _lastCandCellColumn.get(coarsenedY);
						x = _sc.getOriginalXCoordinate(lastCandX, true);
					}
				}
				result = _prefixSum.getElement(x, y);
			}
			return result;
		}
	}
	
	@Override
	public int getMinHalfPerimeterForWeight(double weight) {
		// hp is halfPerimeter
		// Weight = a * hp + b * freq
		// hp >= 2 * (sqrt(freq) - 1); the minimum halfPerimeter is for cross-product
		// Weight <= a * 2 * (sqrt(freq) - 1) + b * freq
		// Solve this quadratic equation to get freq
		// Then hp >= 2 * (sqrt(freq) - 1)
		
		throw new RuntimeException("Not implemented for now!");
	}
}