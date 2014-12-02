package plan_runner.ewh.algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.ewh.algorithms.optimality.WeightFunction;
import plan_runner.ewh.data_structures.JoinMatrix;
import plan_runner.ewh.data_structures.Region;
import plan_runner.ewh.data_structures.UJMPAdapterByteMatrix;
import plan_runner.ewh.visualize.UJMPVisualizer;
import plan_runner.ewh.visualize.VisualizerInterface;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;

/* If we receive input tuples:
 *     * Postponed algorithm:
 *         ** Sort them
 *         ** To be able to answer arbitrary query size question, run precomputation 
 *              *** O(n^2) complexity
 *              *** can be made cheaper, but no need: it's not on the critical path
 *         ** Run Balance for Output (pxq partitioning) algorithm     
 *     * Keeping everything sorted (not on critical path):
 *         ** Advantage: 
 *              *** No need to do sort
 *         ** Disadvantages:     
 *              *** Data structure is complicated and replicated:
 *                     **** It is more complicated than two Btrees, as we need to keep outputs as well
 *                     **** Rows: Sorted list (rows) of sorted lists (outputs)
 *                     **** Columns: Sorted list (columns) of sorted lists (outputs)
 *              *** Complexity: O(nlogn) to insert, and O(nlogn) to join with
 *         ** Every region query size is O(logm), instead of O(1); m is the number of output tuples 
 * 
 *         
 * If we receive output tuples:
 *     * Postponed algorithm:
 *         ** Take input tuples of the output tuple, and do the same as in "If we receive input tuples:"
 *     * Keeping everything sorted (not on critical path):
 *         ** The same as "If we receive input tuples:", except that we do not compute the output tuple
 *         
 */   
public class OutputShallowCoarsener extends ShallowCoarsener{
private static Logger LOG = Logger.getLogger(OutputShallowCoarsener.class);
	private int _originalXSize;
	private int _originalYSize;
	protected Map _map;
	
	private int _totalEnters = 0;
	
	protected int _numXBuckets, _numYBuckets;
	private List<BucketBoundary> _bbListX, _bbListY;
	private Map<Integer, Integer> _originalToCoarsenedX = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> _originalToCoarsenedY = new HashMap<Integer, Integer>();
	
	private List<Double> _rowWeights, _columnWeights;
	private double _rowWeightsSum, _columnWeightsSum; // updated from multiplyWeights
	
	// exists only for ITER_MODE.MAX_ITER (when looking for MAX_WEIGHT among all the rounded matrix cells)
	private List<BucketBoundary> _bestBbListX, _bestBbListY;
	private double _maxWeightEver = Integer.MAX_VALUE;
	
	protected enum ADJUST_MODE{
		// "backup-restore" is just slightly faster (they are almost the same), 
		//     but "adjust to numBuckets" partitions the smaller relation more nicely	
		// "backup-restore" is exactly how they say the algorithm should look like; 
		//     one can think of "adjust to numBuckets" as an optimization
		BACKUP_RESTORE,
		ADJUST_NUM_BUCKETS // BETTER
		// Another way: Given list of weights, iteratively:
		//    * extract those single-liners which have weight > avg; assign them to a single bb
		//    * recompute avg over the rest of the relation
		// This is invoked only if we have insufficient bbList.size()
		//   Probably no better than ADJUST_NUM__BUCKETS
		// TODO: maybe the right way to do this would be to modify the algorithm,
		//         such that we multiply by some factor proportionally to (originalXSize - numXBuckets)
	}
	private ADJUST_MODE _amode;
	
	protected enum ITER_MODE{
		MAX_WEIGHT, // stop after all the cells are beyond certain maximum (hard to know it ahead of time)
		MAX_ITER // BETTER: stop after certain number of iterations (given by algorithm authors)
		// Example: 
		//    MAX_WEIGHT: maxW = 672, time = 0.55s, #iter = 1163, totalEnters = 14006
		//    MAX_ITER  : maxW = 649, time = 0.7s, #iter = 1524, totalEnters = 12670
	}
	private ITER_MODE _imode;

	// backup-restore data structures
	private List<BucketBoundary> _bbBackupX, _bbBackupY;
	private List<Double> _rowBackupWeights, _columnBackupWeights; // variable X from the algorithm
	private int _backupRowLower, _backupRowUpper, _backupColumnLower, _backupColumnUpper;

	protected WeightPrecomputation _wp;
	protected WeightFunction _wf;
	
	protected final double MAX_WEIGHT_BADNESS = 0.5;
	protected final double EPSILON = 0.1;
	
	private int ITER_MULTIPLIER = 1;
	
	public OutputShallowCoarsener(int numXBuckets, int numYBuckets, WeightFunction wf, Map map, ADJUST_MODE amode, ITER_MODE imode){
		_map = map;
		_amode = amode;
		_imode = imode;
		setIModeConf();
		
		_numXBuckets = numXBuckets;
		_numYBuckets = numYBuckets;
		
		_wf = wf;
		
		if(SystemParameters.isExisting(map, "ROUNDING_OUTPUT_ITER_MULT")){
			ITER_MULTIPLIER = SystemParameters.getInt(map, "ROUNDING_OUTPUT_ITER_MULT");
			LOG.info("Setting ROUNDING_OUTPUT_ITER_MULT to " + ITER_MULTIPLIER);
		}
	}
	
	public OutputShallowCoarsener(int numXBuckets, int numYBuckets, WeightFunction wf, Map map){
		this(numXBuckets, numYBuckets, wf, map, ADJUST_MODE.ADJUST_NUM_BUCKETS, ITER_MODE.MAX_ITER);
		//this(numXBuckets, numYBuckets, wf, map, ADJUST_MODE.ADJUST_NUM_BUCKETS, ITER_MODE.MAX_WEIGHT);
	}
	
	// this method overrides whatever was asked in the constructor
	private void setIModeConf(){
		if(SystemParameters.isExisting(_map, "ROUNDING_MODE")){
			String rmode = SystemParameters.getString(_map, "ROUNDING_MODE");
			if(rmode.equalsIgnoreCase("MAX_ITER")){
				_imode = ITER_MODE.MAX_ITER;
				LOG.info("Setting _imode to ITER_MODE.MAX_ITER");
			}else if (rmode.equalsIgnoreCase("MAX_WEIGHT")){
				_imode = ITER_MODE.MAX_WEIGHT;
				LOG.info("Setting _imode to ITER_MODE.MAX_WEIGHT");
			}else{
				throw new RuntimeException("Unrecognized ROUNDING_MODE = " + rmode);
			}
		}
	}
	
	// has to be invoked before all other methods
	public void setOriginalMatrix(JoinMatrix originalMatrix, StringBuilder sb){	
		_originalMatrix = originalMatrix;
		_originalXSize = originalMatrix.getXSize();
		_originalYSize = originalMatrix.getYSize();
		
		// corner case: more buckets than elements in the original matrix
		if(_numXBuckets > _originalXSize){
			_numXBuckets = _originalXSize;
			sb.append("\nWARNING: Bucket size X reduced to the number of rows ").append(_numXBuckets).append("\n");
		}
		if(_numYBuckets > _originalYSize){
			_numYBuckets = _originalYSize;
			sb.append("\nWARNING: Bucket size Y reduced to the number of columns ").append(_numYBuckets).append("\n");
		}
		
		computeCoarsenedMatrix(sb);
		precomputeOriginalToCoarsened();
	}
	
	/*
	// the same as in input
	private void computeCoarsenedMatrix(JoinBooleanMatrixInterface originalMatrix, StringBuilder sb) {
		_rbListX = new ArrayList<BucketBoundary>();
		_rbListY = new ArrayList<BucketBoundary>();
		
		// x
		int bucketXSize = _originalXSize / _numXBuckets;
		for(int i = 0 ; i < _numXBuckets - 1; i++){
			BucketBoundary bb = new BucketBoundary(i * bucketXSize, (i+1) * bucketXSize - 1);
			_rbListX.add(bb);
		}
		BucketBoundary bb = new BucketBoundary((_numXBuckets - 1) * bucketXSize, _originalXSize - 1);
		_rbListX.add(bb);
		
		// y
		int bucketYSize = _originalYSize / _numYBuckets;
		for(int i = 0 ; i < _numYBuckets - 1; i++){
			bb = new BucketBoundary(i * bucketYSize, (i+1) * bucketYSize - 1);
			_rbListY.add(bb);
		}
		bb = new BucketBoundary((_numYBuckets - 1) * bucketYSize, _originalYSize - 1);
		_rbListY.add(bb);
	}*/	

	/*
	 * This is implementation of pxq algorithm; 
	 *    Given p,q, we try to build best matrix (one with the smallest maxWeight GridCell region)
	 */
	private void computeCoarsenedMatrix(StringBuilder sb) {
		long start = System.currentTimeMillis();
		if(SystemParameters.MONOTONIC_PRECOMPUTATION){
			_wp = new DenseMonotonicWeightPrecomputation(_wf, _originalMatrix, _map);
		}else{
			_wp = new DenseWeightPrecomputation(_wf, _originalMatrix);	
		}
		long endPrecomputation = System.currentTimeMillis();
		double elapsedPrecomputation = (endPrecomputation - start) / 1000.0;

		double maxGridCellWeight = -1;
		int maxIter = -1;
		if(_imode == ITER_MODE.MAX_WEIGHT){
			start = System.currentTimeMillis();
			maxGridCellWeight = computeMaxGridCellWeight();
			double elapsed = (System.currentTimeMillis() - start) / 1000.00;
			LOG.info("We aim to get maxGridCellWeight = " + maxGridCellWeight + " and it takes " + elapsed + " seconds.");
		}else if(_imode == ITER_MODE.MAX_ITER){
			maxIter = computeMaxIter();
			LOG.info("Rounding output is in MaxIter mode!");
		}
		int iteration = 0;
		
		initializeRowColumnWeights(); // A1
		while(true){
			computeAlphaGoodPartitioning(iteration); // A2A
			
			Region chosenRegion = null; //A2B
			if(_imode == ITER_MODE.MAX_WEIGHT){
				chosenRegion = findOverweighted(maxGridCellWeight, iteration);
				if (chosenRegion == null){
					// noone is above the cost; we are done
					break;
				}
			}else if (_imode == ITER_MODE.MAX_ITER){
				if(iteration == maxIter){
					break;
				}
				chosenRegion = findMaxWeightRegion(iteration);
				if(chosenRegion == null){
					// There is no candidate region in the rounded matrix spanning on at least 2 sample matrix cells
					StringBuilder errMsg = new StringBuilder();
					errMsg.append("There is no candidate region in the rounded matrix spanning on at least 2 sample matrix cells!\n");
					errMsg.append("   We throw an exception, because if you are here, probably you will get bad running time.\n");
					errMsg.append("   Try to increase the sample size and increase sample matrix size (n_s); maybe even decrease the rounded matrix size (n_r).");
					throw new RuntimeException(errMsg.toString());
					/*
					 * Alternatively, issue a warning and break
					LOG.info("WARNING! WARNING! WARNING! WARNING! ");
					break;
					*/
				}
			}
			
			multiplyWeights(chosenRegion);//A2C
			
			//LOG.info("Iteration " + iteration + " completed.");
			iteration++;
		}
		
		if(_imode == ITER_MODE.MAX_ITER){
			// continue with bucket boundaries which led to minimum maximum region weight (weight in the work/BSP sense)
			_bbListX = _bestBbListX;
			_bbListY = _bestBbListY;
		}
		
		LOG.info("Finished after " + iteration + " iterations.");
		LOG.info("Total enters " + _totalEnters);
		LOG.info("Final sample matrix separators are " + getSeparatorsString());
		
		double elapsedRounding = (System.currentTimeMillis() - endPrecomputation) / 1000.0;
		LOG.info("Part of BSP algorithm: Precomputation takes " + elapsedPrecomputation + " seconds, and rounding pxp algorithm takes "+ elapsedRounding + " seconds.");
		sb.append("\nPrecomputation takes ").append(elapsedPrecomputation).append(" seconds, " +
				"and rounding pxp algorithm takes ").append(elapsedRounding).append(" seconds.\n");
	}

	//A1
	private void initializeRowColumnWeights(){
		_rowWeights = new ArrayList<Double>();
		_columnWeights = new ArrayList<Double>();
		
		double weight = 1;
		for(int i = 0; i < _originalXSize; i++){
			_rowWeights.add(weight);
		}
		for(int j = 0; j < _originalYSize; j++){
			_columnWeights.add(weight);
		}
		
		_rowWeightsSum = weight * _rowWeights.size();
		_columnWeightsSum = weight * _columnWeights.size();
	}
	
	/*
	 * Partitions row/columnWeights into p/q partitions
	 * A2A
	 */
	private void computeAlphaGoodPartitioning(int iteration) {
		_bbListX = new ArrayList<BucketBoundary>();
		_bbListY = new ArrayList<BucketBoundary>();
		_bbListX = computeAlphaGoodPartitioning(_rowWeights, _rowWeightsSum, _numXBuckets, _originalXSize, _bbListX, iteration);
		_bbListY = computeAlphaGoodPartitioning(_columnWeights, _columnWeightsSum, _numYBuckets, _originalYSize, _bbListY, iteration);

		if(_amode == ADJUST_MODE.BACKUP_RESTORE){
			if(_bbListX.size() < _numXBuckets){
				restoreX();
			}
			if(_bbListY.size() < _numYBuckets){
				restoreY();
			}	
		}
	}

	/*
	 * According to the paper
	 *   Do not allow weight of a tile to go over the average
	 *   The best case for load-balancing may be to slightly better than the average
	 *   But then we might end up having less buckets than required
	 */
	private List<BucketBoundary> computeAlphaGoodPartitioning(List<Double> weights, double weightsSum, int numBuckets, int originalSideSize, List<BucketBoundary> bbList, int iteration) {
		// double maxSumRowWeight = getSumElements(weights) / numBuckets;
		double maxSumRowWeight = weightsSum / numBuckets;
		double currentSumRowWeight = 0;
		int previousDivider = 0;
		
		/*
		if(iteration == 358){
			LOG.info("maxSumRowWeight " + maxSumRowWeight);
			LOG.info("weights.size() " + weights.size());
			LOG.info("numBuckets " + numBuckets);
			LOG.info("originalSideSize " + originalSideSize);
		}
		*/
		
		for(int i = 0; i < originalSideSize; i++){
			currentSumRowWeight += weights.get(i);
			if(currentSumRowWeight >= maxSumRowWeight){
				// point of split
				BucketBoundary bb = null;
				if(i == previousDivider){
					// a single actual row surpasses the maximum; each rowTile must take at least one actual row
					bb = new BucketBoundary(previousDivider, i, currentSumRowWeight);
					previousDivider = i + 1;
				}else{
					bb = new BucketBoundary(previousDivider, i - 1, currentSumRowWeight - weights.get(i));
					previousDivider = i;
					i--; // go again through that element (to add to currentSumRowWeight) 
				}
				bbList.add(bb);
				currentSumRowWeight = 0;
				if(bbList.size() == numBuckets - 1){
					// last entry is added separately; it may be bigger than others
					break;
				}
			}
		}
		
		// account for the last one
		if(previousDivider <= originalSideSize - 1){
			currentSumRowWeight = getSumElements(weights, previousDivider, originalSideSize - 1);
			BucketBoundary bb = new BucketBoundary(previousDivider, originalSideSize - 1);
			bbList.add(bb);
		}
			
		if(_amode == ADJUST_MODE.ADJUST_NUM_BUCKETS){
			while(bbList.size() < numBuckets){
				_totalEnters++;
				bbList = adjustToNumBuckets(weights, bbList, numBuckets);
			}

			// if there is a discrepancy, there is something wrong
			if(bbList.size() != numBuckets){
				throw new RuntimeException("The number of generated buckets " + bbList.size() + " differ from the number of required buckets " + numBuckets + "!");
			}
		}
		
		return bbList;
	}
	
	private List<BucketBoundary> adjustToNumBuckets(List<Double> weights, List<BucketBoundary> bbList, int numBuckets) {
		
		// find the top numBuckets bbs which have more than 1 row/column
		List<BucketBoundary> bbSpanMultiple = new ArrayList<BucketBoundary>();
		
		for(int i = 0; i < bbList.size(); i++){
			BucketBoundary bb = bbList.get(i);
			int lower = bb.getLowerPos();
			int upper = bb.getUpperPos();
			if (lower != upper){
				bb.setIndex(i);
				bbSpanMultiple.add(bb);
			}
		}
		Collections.sort(bbSpanMultiple, new BBWeightComparator());
		List<BucketBoundary> bbTopSpanMultiple = new ArrayList<BucketBoundary>();
		
		int bucketsToSplit = numBuckets - bbList.size();	
		/*
		LOG.info("bbSpanMultiple.size() " + bbSpanMultiple.size());
		LOG.info("bucketsToSplit " + bucketsToSplit);
		LOG.info("bbList.size " + bbList.size());
		LOG.info("numBuckets " + numBuckets);
		*/
		
		if(bucketsToSplit > bbSpanMultiple.size()){
			bucketsToSplit = bbSpanMultiple.size();
		}
		for(int i = 0; i < bucketsToSplit; i++){
			bbTopSpanMultiple.add(bbSpanMultiple.get(i));
		}
		
		// now order them according to the index
		Collections.sort(bbTopSpanMultiple, new BBIndexComparator());
		
		// now add them to the new collection
		List<BucketBoundary> result = new ArrayList<BucketBoundary>();
		int previousIndex = 0;
		for(int i = 0; i < bbTopSpanMultiple.size(); i++){
			BucketBoundary bbSplit = bbTopSpanMultiple.get(i);
			int currentIndex = bbSplit.getIndex();
			for(int j = previousIndex; j < currentIndex; j++){
				// adding non-divided bbs
				result.add(bbList.get(j));
			}
			addSplit(weights, result, bbSplit);
			previousIndex = currentIndex + 1;
		}
		
		for(int i = previousIndex; i < bbList.size(); i++){
			result.add(bbList.get(i));
		}
		
		return result;
	}

	private void addSplit(List<Double> weights, List<BucketBoundary> result, BucketBoundary bbSplit) {
		int lowerPos = bbSplit._lowerPos;
		int upperPos = bbSplit._upperPos;
		double tileWeight = bbSplit.getWeight();
		
		double halfTileWeight = 0;
		int divisor = -1; // inclusive
		for(int i = lowerPos; i < upperPos; i++){
			halfTileWeight += weights.get(i);
			if(halfTileWeight >= tileWeight/2){
				divisor = i;
				break;
			}
		}
		
		if(divisor == -1){
			divisor = upperPos - 1;
		}
		
		BucketBoundary bb1 = new BucketBoundary(lowerPos, divisor);
		BucketBoundary bb2 = new BucketBoundary(divisor + 1, upperPos);
		
		result.add(bb1);
		result.add(bb2);
	}
	
	// A2B
	private Region findMaxWeightRegion(int iteration) {
		double maxWeight = 0;
		Region maxRegion = null;

		int firstCandInLastLine = 0;
		for(int i = 0; i < _bbListX.size(); i++){
			boolean isFirstInLine = true;
			int x1 = _bbListX.get(i).getLowerPos();
			int x2 = _bbListX.get(i).getUpperPos();
			for(int j = firstCandInLastLine; j < _bbListY.size(); j++){
				int y1 = _bbListY.get(j).getLowerPos();
				int y2 = _bbListY.get(j).getUpperPos();
				
				//these are sample matrix coordinates
				Region region = new Region(x1, y1, x2, y2);
				double weight = getWeightEmpty0(region);
				//weight = 0 iff region is non-candidate
				boolean isCandidate = (weight > 0);
				if(isCandidate){
					if(region.getHalfPerimeter() > 2 && weight > maxWeight){
						// we can reduce the size of region only if it is bigger than 1x1 (sample matrix size)
						maxWeight = weight;
						maxRegion = region;
					}
					if(isFirstInLine){
						firstCandInLastLine = j;
						isFirstInLine = false;
					}
				}
				if(!isFirstInLine && !isCandidate){
					// I am right from the candidate are; the first non-candidate guy means I should switch to the next row
					break;
				}
			}
		}
		
		if(maxWeight < _maxWeightEver){
			//save best weight ever and the corresponding bucket boundaries
			_maxWeightEver = maxWeight;
			_bestBbListX = _bbListX;
			_bestBbListY = _bbListY;
		}
		
		return maxRegion;
	}
	
	private Region findOverweighted(double maxGridCellWeight, int iteration) {
		int firstCandInLastLine = 0;
		for(int i = 0; i < _bbListX.size(); i++){
			boolean isFirstInLine = true;
			int x1 = _bbListX.get(i).getLowerPos();
			int x2 = _bbListX.get(i).getUpperPos();
			for(int j = firstCandInLastLine; j < _bbListY.size(); j++){
				int y1 = _bbListY.get(j).getLowerPos();
				int y2 = _bbListY.get(j).getUpperPos();
				
				//these are sample matrix coordinates				
				Region region = new Region(x1, y1, x2, y2);
				double weight = getWeightEmpty0(region);
				//weight = 0 iff region is non-candidate
				boolean isCandidate = (weight > 0);
				if(isCandidate){
					if(region.getHalfPerimeter() > 2 && weight > maxGridCellWeight){
						// we can reduce the size of region only if it is bigger than 1x1 (sample matrix size)
						return region;
					}
					if(isFirstInLine){
						firstCandInLastLine = j;
						isFirstInLine = false;
					}
				}
				if(!isFirstInLine && !isCandidate){
					// I am right from the candidate are; the first non-candidate guy means I should switch to the next row
					break;
				}
			}
		}
		return null;
	}
	
	//A2C
	private void multiplyWeights(Region overweighted){
		double beta = 1 + EPSILON / 2;
		
		int x1 = overweighted.get_x1();
		int y1 = overweighted.get_y1();
		int x2 = overweighted.get_x2();
		int y2 = overweighted.get_y2();

		if(_originalXSize > _numXBuckets){
			// if its equal, no need to change weights
			if(_amode == ADJUST_MODE.BACKUP_RESTORE){
				backupX(x1, x2);
			}
			if(x1 != x2){
				// if the bucket is only 1 line wide, we cannot divide it further
				for(int i = x1; i <= x2; i++){
					double deltaWeight = multiplyElement(_rowWeights, i, beta);
					_rowWeightsSum += deltaWeight;
				}
			}
		}

		if(_originalYSize > _numYBuckets){
			// if its equal, no need to change weights
			if(_amode == ADJUST_MODE.BACKUP_RESTORE){
				backupY(y1, y2);
			}
			if(y1 != y2){
				// if the bucket is only 1 line wide, we cannot divide it further
				for(int j = y1; j <= y2; j++){
					double deltaWeight = multiplyElement(_columnWeights, j, beta);
					_columnWeightsSum += deltaWeight;
				}
			}
		}
	}

	private int computeMaxIter() {
		double N = ((double)_originalXSize) * _originalYSize;
		int p = _numXBuckets;
		int q = _numYBuckets;
		// interesting formula: logN / log2 = log_2_N
		return ITER_MULTIPLIER * (int)( (p + q) * Math.log(N));
	}
	
	// the following methods I might want to overload
	// weight = b * output
	protected double computeMaxGridCellWeight(){
		int minSideLength = MyUtilities.getMin(_numXBuckets, _numYBuckets);
		
		// in the worst case, maxGridCellWeight is
		//    * for pxp partitioning, m/p
		//    * for pxq partitioning, m/(min(p,q))
		double result = MAX_WEIGHT_BADNESS * (2 + EPSILON) * _originalMatrix.getTotalNumOutputs() / minSideLength;
		//LOG.info("Max grid cell weight is " + result);
		return result;
	}

	// weight = b * output
	protected double getWeight(Region region){
		return _wp.getFrequency(region);
	}
	
	// used in InputOutputShallowCoarsener
	//   returns 0 if the region contains no output
	protected double getWeightEmpty0(Region region){
		return getWeight(region);
	}

	@Override
	public WeightPrecomputation getPrecomputation() {
		return _wp;
	}
	
	/* The following 6 methods are called from outside,
	 *    when this algorithm is completed (has numXBuckets x numYBuckets) 
	 */
	@Override
	public int getNumXCoarsenedPoints(){
		return _numXBuckets;
	}
	
	@Override
	public int getNumYCoarsenedPoints(){
		return _numYBuckets;
	}
	
	
	@Override
	public int getOriginalXCoordinate(int ci, boolean isHigher) {
		if(isHigher){
			return _bbListX.get(ci).getUpperPos();
		}else{
			return _bbListX.get(ci).getLowerPos();
		}
	}

	@Override
	public int getOriginalYCoordinate(int cj, boolean isHigher) {
		if(isHigher){
			return _bbListY.get(cj).getUpperPos();
		}else{
			return _bbListY.get(cj).getLowerPos();
		}
	}
	
	// in order to avoid invoking binary search every time
	private void precomputeOriginalToCoarsened() {
		precomputeOriginalToCoarsened(_bbListX, _originalToCoarsenedX);
		precomputeOriginalToCoarsened(_bbListY, _originalToCoarsenedY);
	}

	private void precomputeOriginalToCoarsened(List<BucketBoundary> bbList, Map<Integer, Integer> originalToCoarsened) {
		for(int i = 0; i < bbList.size(); i++){
			int lowerPos = bbList.get(i).getLowerPos();
			int upperPos = bbList.get(i).getUpperPos();
			originalToCoarsened.put(lowerPos, i);
			originalToCoarsened.put(upperPos, i);
		}
	}

	
	@Override
	public int getCoarsenedXCoordinate(int x) {
		if(_originalToCoarsenedX.containsKey(x)){
			return _originalToCoarsenedX.get(x);
		}else{
			//here when invoked from getEnclosingRegion or BSP.DENSE
			//DenseMonotonicWeightPrecomputation.precompute invokes computePrefix invokes computePrefixUp invokes _sc.getCoarsenedXCoordinate
	        //     In the second precomputation, _sc is OutputShallowCoarsener
	        //     Should never be in the else branch of OutputShallowCoarsener.getCoarsenedYCoordinate, unless secondPrecomputation is DENSE or BOTH
			int lowerBound = 0;
			int upperBound = _numXBuckets;
			while(lowerBound <= upperBound){
				int currentPos = (lowerBound + upperBound) / 2;
				BucketBoundary bb = _bbListX.get(currentPos); 
				int dataLower = bb.getLowerPos();
				int dataUpper = bb.getUpperPos();
				if(x >= dataLower && x <= dataUpper){
					return currentPos;
				}else if (x > dataUpper){
					lowerBound = currentPos + 1;
				}else if(x < dataLower){
					upperBound = currentPos - 1;
				}
			}
			throw new RuntimeException("Element " + x + " not found!");
		}
	}

	@Override
	public int getCoarsenedYCoordinate(int y) {
		if(_originalToCoarsenedY.containsKey(y)){
			return _originalToCoarsenedY.get(y);
		}else{
			//here when invoked from getEnclosingRegion or BSP.DENSE
			//DenseMonotonicWeightPrecomputation.precompute invokes computePrefix invokes computePrefixUp invokes _sc.getCoarsenedXCoordinate
	        //     In the second precomputation, _sc is OutputShallowCoarsener
	        //     Should never be in the else branch of OutputShallowCoarsener.getCoarsenedYCoordinate, unless secondPrecomputation is DENSE or BOTH
			int lowerBound = 0;
			int upperBound = _numYBuckets;
			while(lowerBound <= upperBound){
				int currentPos = (lowerBound + upperBound) / 2;
				BucketBoundary bb = _bbListY.get(currentPos); 
				int dataLower = bb.getLowerPos();
				int dataUpper = bb.getUpperPos();
				if(y >= dataLower && y <= dataUpper){
					return currentPos;
				}else if (y > dataUpper){
					lowerBound = currentPos + 1;
				}else if(y < dataLower){
					upperBound = currentPos - 1;
				}
			}
			throw new RuntimeException("Element " + y + " not found!");
		}
	}
	
	@Override
	public String toString(){
		return "OutputBinaryCoarsener [numXBuckets = " + _numXBuckets + ", numYBuckets = " + _numYBuckets + "]"; 
	}
	
	 // backup of bbList, weight, lower, upper bound
	private void backupX(int x1, int x2) {
		_bbBackupX = _bbListX;
		_backupRowLower = x1;
		_backupRowUpper = x2;
		_rowBackupWeights = new ArrayList<Double>();
		for(int i = x1; i <= x2; i++){
			_rowBackupWeights.add(_rowWeights.get(i));
		}
	}
	
	private void backupY(int y1, int y2) {
		_bbBackupY = _bbListY;
		_backupColumnLower = y1;
		_backupColumnUpper = y2;
		_columnBackupWeights = new ArrayList<Double>();
		for(int i = y1; i <= y2; i++){
			_columnBackupWeights.add(_columnWeights.get(i));
		}
	}	

	// restore of bbList, weight, lower, upper bound
	private void restoreX() {
		_bbListX = _bbBackupX;
		for(int i = _backupRowLower; i <= _backupRowUpper; i++){
			_rowWeights.set(i, _rowBackupWeights.get(i - _backupRowLower));
		}
	}
	
	private void restoreY() {
		_bbListY = _bbBackupY;
		for(int i = _backupColumnLower; i <= _backupColumnUpper; i++){
			_columnWeights.set(i, _columnBackupWeights.get(i - _backupColumnLower));
		}
	}
	
	// not used anymore
	private double getSumElements(List<Double> lst){
		double result = 0;
		for(double el: lst){
			result += el;
		}
		return result;
	}
	
	// start and end are inclusive
	private double getSumElements(List<Double> lst, int start, int end){
		double result = 0;
		for(int i = start; i <= end; i++){
			result += lst.get(i);
		}
		return result;
	}
	
	private double multiplyElement(List<Double> weights, int position, double multiplier) {
		double originalValue = weights.get(position);
		double newValue = originalValue * multiplier;
		weights.set(position, newValue);
		return newValue - originalValue;
	}
	
	protected int findMin(int num1, int num2){
		if (num1 < num2){
			return num1;
		}else{
			return num2;
		}
	}
	
	private List<Region> getSeparators(){
		List<Region> regions = new ArrayList<Region>();
		
		for(int i = 0; i < _bbListX.size(); i++){
			int upperPos = _bbListX.get(i).getUpperPos();
			Region region = new Region(upperPos, 0, upperPos, _originalYSize - 1);
			regions.add(region);
		}
		
		for(int j = 0; j < _bbListY.size(); j++){			
			int upperPos = _bbListY.get(j).getUpperPos();
			Region region = new Region(0, upperPos, _originalXSize - 1, upperPos);
			regions.add(region);
		}
		
		return regions;
	}
	
	private String getSeparatorsString(){
		StringBuilder sb = new StringBuilder();
		
		sb.append("\nX upperPositions (inclusive) are: ");
		for(int i = 0; i < _bbListX.size(); i++){
			int upperPos = _bbListX.get(i).getUpperPos();
			sb.append("\n").append(upperPos);
		}
		
		sb.append("\nY upperPositions (inclusive) are: ");
		for(int j = 0; j < _bbListY.size(); j++){			
			int upperPos = _bbListY.get(j).getUpperPos();
			sb.append("\n").append(upperPos);
		}
		
		return sb.toString();
	}
	
	public static void main(String[] args) throws IOException{
		// parameters
		ADJUST_MODE amode = ADJUST_MODE.ADJUST_NUM_BUCKETS;
		ITER_MODE imode = ITER_MODE.MAX_ITER;
		boolean toVisualize = false;
		String matrixName = "theta_tpch5_R_N_S_L_z2";
		//String matrixName = "theta_hyracks_z0";
		
		//instantiate matrix
		String matrixRoot = "test/join_matrix/";
		JoinMatrix joinMatrix = new UJMPAdapterByteMatrix(matrixRoot, matrixName);
		
		//visualize the original matrix
		if(toVisualize){
			String label = matrixName;
			VisualizerInterface visualizer = new UJMPVisualizer(label);
			joinMatrix.visualize(visualizer);
		}
		
		//actual work
		StringBuilder sb = new StringBuilder();
		OutputShallowCoarsener coarsener = new OutputShallowCoarsener(50, 50, new WeightFunction(1,1), null, amode, imode);
		coarsener.setOriginalMatrix(joinMatrix, sb);
		LOG.info(sb.toString());
		
		LOG.info(coarsener.getSeparatorsString());
		
		Region maxRegion = coarsener.findMaxWeightRegion(-1);
		LOG.info("Max Region weight is " + coarsener.getWeight(maxRegion));
		
		// visualize with separators
		// UJMP bug: The matrix with regions does not show the output
		if(toVisualize){
			JoinMatrix matrixCopy = joinMatrix.getDeepCopy(); // in order not to spoil the original
			List<Region> separators = coarsener.getSeparators();
			matrixCopy.setRegions(separators);
			String label = matrixName + " with regions";
			VisualizerInterface visualizer = new UJMPVisualizer(label);
			matrixCopy.visualize(visualizer);
		}
	}
	
	private class BucketBoundary{
		// boundaries are inclusive, as in regions
		private int _lowerPos, _upperPos;
		private double _weight;
		private int _index; // index in the collection
		
		public BucketBoundary(int lowerPos, int upperPos){
			_lowerPos = lowerPos;
			_upperPos = upperPos;
		}
		
		public BucketBoundary(int lowerPos, int upperPos, double weight){
			this(lowerPos, upperPos);
			_weight = weight;
		}
		
		public BucketBoundary(BucketBoundary other){
			_lowerPos = other._lowerPos;
			_upperPos = other._upperPos;
			_weight = other._weight;
			_index = other._index;
		}

		public int getLowerPos() {
			return _lowerPos;
		}

		public void setLowerPos(int lowerPos) {
			_lowerPos = lowerPos;
		}

		public int getUpperPos() {
			return _upperPos;
		}

		public void setUpperPos(int upperPos) {
			_upperPos = upperPos;
		}
		
		public void setWeight(int weight){
			_weight = weight;
		}
		
		public double getWeight(){
			return _weight;
		}
		
		public void setIndex(int index){
			_index = index;
		}
		
		public int getIndex(){
			return _index;
		}
	}
	
	private class BBWeightComparator implements Comparator<BucketBoundary>{
		@Override
		public int compare(BucketBoundary bb1, BucketBoundary bb2){
			if(bb1._weight < bb2._weight){
				return 1;
			}else if(bb1._weight > bb2._weight){
				return -1;
			}else{
				return 0;
			}
		}
	}
	
	private class BBIndexComparator implements Comparator<BucketBoundary>{
		@Override
		public int compare(BucketBoundary bb1, BucketBoundary bb2){
			if(bb1._index < bb2._index){
				return -1;
			}else if(bb1._index > bb2._index){
				return 1;
			}else{
				return 0;
			}
		}
	}	
}