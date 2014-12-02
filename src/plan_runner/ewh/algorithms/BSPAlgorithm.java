package plan_runner.ewh.algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import plan_runner.conversion.NumericConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.ewh.algorithms.optimality.MaxAvgOptimality;
import plan_runner.ewh.algorithms.optimality.OptimalityMetricInterface;
import plan_runner.ewh.algorithms.optimality.WeightFunction;
import plan_runner.ewh.data_structures.ExtremePositions;
import plan_runner.ewh.data_structures.JoinMatrix;
import plan_runner.ewh.data_structures.MatrixIntInt;
import plan_runner.ewh.data_structures.Region;
import plan_runner.ewh.data_structures.SimpleMatrix;
import plan_runner.ewh.data_structures.TooSmallMaxWeightException;
import plan_runner.ewh.main.PushStatisticCollector;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;

/*
 * For growing size regions (1x1, 2x1, 2x2, etc)
      ** if frequency = 0, #rectangles = 0
      ** else 
             find smallest candidate containing region
             if weight < CONSTANT, output is # rectangles = 1, List<CandRegions>
             else try best horizontal or vertical partitioning, output is #rectangles, List<CandRegions>
  * Output is partitioning on the original matrix (the biggest rectangle)
  * Run binary search to get desired #rectangles = J
       ** Lower bound for J joiners is Wmin = (a*2*sqrt(freq) + b*freq)/J
       ** Upper bound is Wmax = a * (|R| + |S|) + b*freq

  * Overall
  *   A1: We do not add regions with weight <= maxWeight
  *     All the other regions (both candidate and non-candidate) are added.
  *     These regions have at least 2 rectangles
 */
public class BSPAlgorithm implements TilingAlgorithm{
	private static Logger LOG = Logger.getLogger(BSPAlgorithm.class);	
	
	private Map _map;
	private int _j;
	
	// build based on _wf; it's the precomputation of the original matrix	
	private WeightPrecomputation _wp;
	private WeightFunction _wf;
	
	// joinCondition, necessary for figuring out who is a candidate
	private ComparisonPredicate _cp;
	
	private ShallowCoarsener _coarsener;
	private StringBuilder _sb;
	
	// smallest value for double
	private final double DELTA = 0.01;
	
	// how many times to encounter _j joiners to exit the loop
	// increase this constant to get possibly better load balance (lower actual maxRegionWeight)
	// used in binary search
	private final int EXACT_ENCOUNTERED_COUNTER_BREAK = 1;
	// For large number of joiners, it may take too much time to reach exactly the required number of joiners
	// If the obtained number of joiners is within CLOSE_PERCENTAGE of the desired number of joiners
	//    we terminate the loop and return the obtained regions
	// To effectively comment this out, just set this to a extremely small number
	private final double CLOSE_PERCENTAGE = 0.05;
	
	// we exit the loop if any result < _j is found, and this many iterations are reached
	private final int MAX_ITERATIONS = 20; //10;
	
	public enum COVERAGE_MODE{
		DENSE, // obsolete: requires less memory in total (not sure)
		SPARSE // complexity is much lower, works nice in practice 
		      //(because it builds rounded matrix only on the sample matrix cell candidates, it expects monotonicity)
	};
	private COVERAGE_MODE _cmode = COVERAGE_MODE.SPARSE;
	
	public enum DIVISION_MODE{
		LINEAR, // theoretically should be slower, but in practice it is faster; tested to a great extent
		LOG    // it is also suspicious that it yields different regions than with LINEAR (theta_hyrack z0)
	}
	//private DIVISION_MODE _dmode = DIVISION_MODE.LINEAR;
	private DIVISION_MODE _dmode = DIVISION_MODE.LOG;
	
	// coarsened points;
	private Map<Segment, ExtremePositions> _segmentExtremes; 
	
	// used only for COVERAGE_MODE.SPARSE
	private SortedMap<Integer, List<Region>> _sccCoarsenedRegions;
		
	private BSPAlgorithm(Map map, int j, WeightFunction wf){
		_map = map;
		_j = j;
		
		_wf = wf;
	}

	public BSPAlgorithm(Map map, int j, WeightFunction wf, ShallowCoarsener coarsener){
		this(map, j, wf);
		_coarsener = coarsener;
	}

	public BSPAlgorithm(Map map, int j, WeightFunction wf, ShallowCoarsener coarsener, COVERAGE_MODE cmode){
		this(map, j, wf, coarsener);
		_cmode = cmode;
	}
	
	@Override
	public WeightPrecomputation getPrecomputation() {
		return _wp;
	}
	
	@Override
	public WeightFunction getWeightFunction(){
		return _wf;
	}

	//noone is calling it as it depends on the context whether the expected region is original or coarsened
	@Override
	public double getWeight(Region region) {
		return _wp.getWeight(region);
	}
	
	@Override
	public List<Region> partition(JoinMatrix joinMatrix, StringBuilder sb) {
		_sb = sb;
		_cp = joinMatrix.getComparisonPredicate();
		_coarsener.setOriginalMatrix(joinMatrix, sb);
		_wp = _coarsener.getPrecomputation();
		if(_wp == null){
			// only for InputShallowCoarsener
			long start = System.currentTimeMillis();
			if(SystemParameters.MONOTONIC_PRECOMPUTATION){
				_wp = new DenseMonotonicWeightPrecomputation(_wf, joinMatrix, _map);
			}else{
				_wp = new DenseWeightPrecomputation(_wf, joinMatrix);
			}
			double elapsed = (System.currentTimeMillis() - start) / 1000.0;
			LOG.info("Part of BSP algorithm: Precomputation done in BSP takes " + elapsed + " seconds.");
			sb.append("\nPrecomputation done in BSP takes ").append(elapsed).append(" seconds.\n");
		}
		
		// assign something to each candidate rounded matrix cell
		long start = System.currentTimeMillis();
		List<Region> candidateRoundedCells = _coarsener.getCandidateRoundedCells(_map);
		double elapsed = (System.currentTimeMillis() - start) / 1000.0;
		LOG.info("Part of BSP algorithm: getCandidateRoundedCells takes " + elapsed + " seconds.");

		// used to avoid expensive entire join matrix weightPrecomputation
		SimpleMatrix deltaCoarsenedMatrix = null;
		if(MyUtilities.isIIPrecPWeight(_map)){
			deltaCoarsenedMatrix = new MatrixIntInt(_coarsener.getNumXCoarsenedPoints(), _coarsener.getNumYCoarsenedPoints());	
		}
		
		int elementsChanged = assignNonZeroToRoundedCandidates(candidateRoundedCells, joinMatrix, deltaCoarsenedMatrix, sb);
		
		if(elementsChanged > 0){
			_wp = recomputePrecomputation(joinMatrix, deltaCoarsenedMatrix);
		}else{
			// even if no change, we have to compute the coarsenedPrecomputation
			if(SystemParameters.COARSE_PRECOMPUTATION){
				start = System.currentTimeMillis();
				 _wp = new PWeightPrecomputation(_wf, _coarsener, _wp);
				elapsed = (System.currentTimeMillis() - start) / 1000.0;
				LOG.info("Part of BSP algorithm: SecondPrecomputation is only pWeightPrecomputation (elements = 0) takes " + elapsed + " seconds.");
			}
		}
		
		// line extremes precomputation
		if(_cmode == COVERAGE_MODE.SPARSE){
			start = System.currentTimeMillis();
			precomputeSegmentExtremes(joinMatrix);
			elapsed = (System.currentTimeMillis() - start)/1000.00;
			LOG.info("Part of BSP algorithm: precomputeSegmentExtremes takes " + elapsed + " seconds.");
			
			start = System.currentTimeMillis();
			_sccCoarsenedRegions = generateSCCCoarsenedRegions();
			elapsed = (System.currentTimeMillis() - start)/1000.00;
			LOG.info("Part of BSP algorithm: generatedSCCCoarsenedRegions takes " + elapsed + " seconds. Total used memory is " + MyUtilities.getUsedMemoryMBs() + " MBs.");
		}
		
		//condition checks
		int numCandidateGridCells = candidateRoundedCells.size();
		LOG.info("Part of BSP algorithm: The number of candidate grid cells in the rounded matrix is " + numCandidateGridCells);
		_sb.append("\nThe number of candidate grid cells in the rounded matrix is ").append(numCandidateGridCells).append(".\n");
		if(numCandidateGridCells < _j){
			// one joiner must have at least one cell
			throw new RuntimeException("Too coarse-grained partitioning, not enough cells!");	
		}
		
		List<Region> regions = binarySearch(joinMatrix);
		if(SystemParameters.getBooleanIfExist(_map, "CHECK_STATISTICS")){
			MyUtilities.compareActualAndSampleJM(joinMatrix, _coarsener, _wp);
		}
		return regions;
	}
	
	// for each candidate query cell which has no output elements, assign one very small output
	//   * joinMatrix.getNumCandidates is based on the number of non-zero entries
	//        ** but this method is used only in Okcan algorithms
	//   * joinMatrix.isEmpty is used nowhere  
	//   * In BSP, we have minimizeToNonEmpty, that's why this method is necessary
	// old name of the method is assignWeightsToCandidateRoundedCells
	private int assignNonZeroToRoundedCandidates(List<Region> candidateRoundedCells, JoinMatrix joinMatrix, SimpleMatrix deltaCoarsenedMatrix, StringBuilder sb){
		int elementsChanged = 0;
		int minPositiveValue = joinMatrix.getMinPositiveValue();
		for(Region region: candidateRoundedCells){
			if(_wp.isEmpty(region)){
				// assign minimal output value to an arbitrary cell within the grid cell
				int x1 = region.get_x1();
				int y1 = region.get_y1();
				joinMatrix.setMinPositiveValue(x1, y1);
				
				if(deltaCoarsenedMatrix != null){
					// used to avoid expensive entire join matrix weightPrecomputation
					int cx1 = _coarsener.getCoarsenedXCoordinate(x1);
					int cy1 = _coarsener.getCoarsenedYCoordinate(y1);
					deltaCoarsenedMatrix.setElement(minPositiveValue, cx1, cy1);
				}

				elementsChanged++;
			}
		}
		
		LOG.info("Part of BSP algorithm: This many candidate rounded cells had no output tuples: " + elementsChanged + ".");
		sb.append("\nThis many candidate grid cells had no output tuples: ").append(elementsChanged).append(".\n");
		
		return elementsChanged;
	}
	
	private WeightPrecomputation recomputePrecomputation(JoinMatrix joinMatrix, SimpleMatrix deltaCoarsenedMatrix){
		WeightPrecomputation denseWP = null;
		PWeightPrecomputation pWP = null;
		MyUtilities.checkIIPrecValid(_map);
		if(MyUtilities.isIIPrecDense(_map)){
			long start = System.currentTimeMillis();
			// compute pWeightPrecomputation by recomputing the entire Dense(Monotonic)WeightPrecomputation
			if(SystemParameters.MONOTONIC_PRECOMPUTATION){
				//needs to use the same coarsener in order to not miss some of the minPositiveValues set from above
				//   namely, the above code may set the minPositiveValue in a sample matrix cell which a different coarsener does not consider a candidate
				denseWP = new DenseMonotonicWeightPrecomputation(_wf, joinMatrix, _map, _coarsener);
			}else{
				denseWP = new DenseWeightPrecomputation(_wf, joinMatrix);
			}
			double elapsed = (System.currentTimeMillis() - start)/1000.0;
			LOG.info("Part of BSP algorithm: Recomputing Dense(Monotonic)WeightPrecomputation (elementsChanged > 0) takes " + elapsed + " seconds.");
			
			start = System.currentTimeMillis();
			if(SystemParameters.COARSE_PRECOMPUTATION){
				denseWP = new PWeightPrecomputation(_wf, _coarsener, denseWP);
			}
			elapsed = (System.currentTimeMillis() - start)/1000.0;
			LOG.info("Part of BSP algorithm: Final PWeightPrecomputation (elementsChanged > 0) takes " + elapsed + " seconds.");
		}
		if(MyUtilities.isIIPrecPWeight(_map)){
			long start = System.currentTimeMillis();
			// COARSE_PRECOMPUTATION must be true
			if(SystemParameters.COARSE_PRECOMPUTATION){
				pWP = new PWeightPrecomputation(_wf, _coarsener, _wp);
				pWP.addDeltaMatrix(deltaCoarsenedMatrix);
			}
			double elapsed = (System.currentTimeMillis() - start)/1000.0;
			LOG.info("Part of BSP algorithm: Computing pWeightPrecomputation (elementsChanged > 0) takes " + elapsed + " seconds.");
		}
		if(MyUtilities.isIIPrecBoth(_map)){
			//implies we entered into the two previous if branches
			long start = System.currentTimeMillis();
			MyUtilities.checkEquality(denseWP, pWP);
			double elapsed = (System.currentTimeMillis() - start)/1000.0;
			LOG.info("Part of BSP algorithm: Checking two WeightPrecomputation (elementsChanged > 0) takes " + elapsed + " seconds.");
		}
		
		if(pWP != null){
			// by default setting pWP
			return pWP;
		}else{
			return denseWP;
		}
	}
	
	private List<Region> binarySearch(JoinMatrix joinMatrix){
		// setting up bounds		
		int xSize = joinMatrix.getXSize();
		int ySize = joinMatrix.getYSize();
		int freq = _wp.getTotalFrequency();
		double singleJoinerLowerBound = _wf.getWeight((int)(2 * Math.sqrt(freq / _j)), freq / _j); // we could use (double)freq, but it is a lower bound anyway
		double singleJoinerUpperBound = _wf.getWeight((xSize + ySize), freq);
		if(_j == 1){
			// necessary to ensure bestRegion!=null when J = 1
			singleJoinerUpperBound += DELTA;
		}
		
		//actual binary search
		double bestAlgMaxWeight = Double.MAX_VALUE;
		double bestActualMaxWeight = Double.MAX_VALUE;
		List<Region> bestRegions = null;
		int exactEncounteredCounter = 0; 
		boolean terminate = false;
		int iterations = 0;
		boolean isFirstEnter = true; // the number of iterations do not change if TooSmallMaxWeightException encountered
		while((singleJoinerLowerBound <= singleJoinerUpperBound) && (exactEncounteredCounter < EXACT_ENCOUNTERED_COUNTER_BREAK) 
				&& (!terminate) && (iterations < MAX_ITERATIONS || bestRegions == null )){
            
			/*
			 * Optimization 1
			// we favor the first trial to smaller value, such that we limit the number of loop iterations
			//    thus, we start from relatively small maxValue(large numJoiners), which is hopefully less than _j
			//          we lose all the benefits if we try too many times with numJoiners > _j
			// This is good for large _j > 8
			double factor = _j / 4.0;
			if (factor < 1) factor = 1;
			double currentAlgMaxWeight = (singleJoinerLowerBound * factor + singleJoinerUpperBound) / (factor + 1.0);
			*/
			
			/*
			 * Optimization 2
			 * The solution is much closer to the singleJoinerLowerBound than to the singleJoinerUpperBound
			 * We want to have normal binary search, but also to:
			 *    Take care not to have too many unsuccessful tries with high number of joiners,
			 *        as on large p (e.g. p = 320) those runs are much longer (19s compared to 5s)
			 */
			int multiplier = 5; // a number which works for low-selectivity joins (e.g. jps)
			double currentAlgMaxWeight = (singleJoinerLowerBound + singleJoinerUpperBound) / 2;
			if(isFirstEnter){
				isFirstEnter = false;
				currentAlgMaxWeight = (singleJoinerLowerBound + multiplier * singleJoinerLowerBound) / 2;
			}
			
			// uncomment when no optimization is used
			//double currentAlgMaxWeight = (singleJoinerLowerBound + singleJoinerUpperBound) / 2;
			LOG.info("CurrentMaxWeight = " + currentAlgMaxWeight + ", binary search in [" + singleJoinerLowerBound + ", " + singleJoinerUpperBound + "]");
						
			// actual work for the given maxWeight
			Map<String, RegionPartitioning> regPart;
			try {
				regPart = drtile(currentAlgMaxWeight, joinMatrix);
			} catch (TooSmallMaxWeightException e) {
				// maxWeight was too small, we need to increase it
				// this is not counted anywhere
				LOG.info("TooSmallMaxWeightException for currentAlgMaxWeight = " + currentAlgMaxWeight);
				singleJoinerLowerBound = currentAlgMaxWeight + DELTA;
				continue;
			}
			int numJoiners = getNumJoiners(regPart, joinMatrix);
			List<Region> regions = getRegions(regPart, joinMatrix);
			OptimalityMetricInterface opt = new MaxAvgOptimality(joinMatrix, regions, _wp);
			_sb.append("\nAt time ").append(PushStatisticCollector.getWallClockTime());
			_sb.append("\nIntermediate result for maxWeight ").append(currentAlgMaxWeight).append(": \n");
			_sb.append(Region.toString(regions, opt, "Intermediate")).append("\n");
			
			// conditions
			if(numJoiners < _j){
				if(MyUtilities.computePercentage(numJoiners, _j) < CLOSE_PERCENTAGE){
					terminate = true;
				}
				
				double currentActualMaxWeight = opt.getActualMaxRegionWeight();
				if(currentActualMaxWeight < bestActualMaxWeight){
					// interestingly, a higher algMaxWeight can yield a lower actualMaxWeight
					//   that's why we optimize for actual weights
					bestActualMaxWeight = currentActualMaxWeight;
					bestAlgMaxWeight = currentAlgMaxWeight;
					bestRegions = regions;
				}
				
				// subtracting a small value such that we can leave the while loop
				singleJoinerUpperBound = currentAlgMaxWeight - DELTA; 
			}else if(numJoiners > _j){
				// adding a small value such that we can leave the while loop
				singleJoinerLowerBound = currentAlgMaxWeight + DELTA;
			}else if(numJoiners == _j){
				exactEncounteredCounter++;

				// we try to find the minimum weight for which we do not have to use _j + 1 joiners
				double currentActualMaxWeight = opt.getActualMaxRegionWeight();
				if(currentActualMaxWeight < bestActualMaxWeight){
					// interestingly, a higher algMaxWeight can yield a lower actualMaxWeight
					//   that's why we optimize for actual weights
					bestActualMaxWeight = currentActualMaxWeight;
					bestAlgMaxWeight = currentAlgMaxWeight;
					bestRegions = regions;
				}

				// there is maybe a smaller maxWeight which yields better load-balance, and the same _j
				singleJoinerUpperBound = currentAlgMaxWeight - DELTA;
				
				// hard to make this generic:
				//   slow down boundary change to remain within these number of joiners
				// singleJoinerUpperBound = singleJoinerLowerBound + (currentMaxWeight - singleJoinerLowerBound) * 2 * 0.7; 
			}
			iterations++;
		}
			
		LOG.info("Total BSP iterations = " + iterations + ", exactEncounteredCounter = " + exactEncounteredCounter + ", terminate = " + terminate);
		_sb.append("\nFinal maxWeight = ").append(bestAlgMaxWeight).append("\n");
		return bestRegions;
		
		/*
		 // Old version which starts from the large maxWeight
		// actual binary search
		double currentMaxWeight = singleJoinerUpperBound;
		Map<String, RegionPartitioning> regPart = drtile(currentMaxWeight, joinMatrix);
		int numJoiners = getNumJoiners(regPart, joinMatrix);
		List<Region> regions = getRegions(regPart, joinMatrix, _wp);
		double lastMaxWeight = 0;
		List<Region> lastRegions = null;
		while (numJoiners < _j){
			_sb.append("\nAt time ").append(PushStatisticCollector.getWallClockTime());
			_sb.append("\nIntermediate result for maxWeight ").append(currentMaxWeight).append(": ").append(Region.toString(regions, _wp)).append("\n");
			
			lastMaxWeight = currentMaxWeight;
			lastRegions = regions;
			
			currentMaxWeight /= 2;
			regPart = drtile(currentMaxWeight, joinMatrix);
			numJoiners = getNumJoiners(regPart, joinMatrix);
			regions = getRegions(regPart, joinMatrix, _wp);
		}
		_sb.append("\nFinal maxWeight = ").append(lastMaxWeight).append("\n");
		return lastRegions;
		*/
		
		/*
		 * Old version which starts from the small maxWeights
		// actual binary search
		double currentMaxWeight = singleJoinerLowerBound;
		Map<String, RegionPartitioning> regPart = drtile(currentMaxWeight, joinMatrix);
		int numJoiners = getNumJoiners(regPart, joinMatrix);
		List<Region> regions = getRegions(regPart, joinMatrix, _wp);
		while (numJoiners > _j){
			_sb.append("\nAt time ").append(PushStatisticCollector.getWallClockTime());
			_sb.append("\nIntermediate result for maxWeight ").append(currentMaxWeight).append(": ").append(Region.toString(regions, _wp)).append("\n");
			
			singleJoinerLowerBound = currentMaxWeight;
			currentMaxWeight *= 2;
			regPart = drtile(currentMaxWeight, joinMatrix);
			numJoiners = getNumJoiners(regPart, joinMatrix);
			regions = getRegions(regPart, joinMatrix, _wp);
		}
		
		// this is only necessary if we want to tweak the max region size in a more fine-grained fashion
		double upperBound = currentMaxWeight;
		if(upperBound > singleJoinerUpperBound){
			upperBound = singleJoinerUpperBound;
		}
		// further call : I believe ratio is 2 if we do not invoke it further
		_sb.append("\nFinal maxWeight = ").append(currentMaxWeight).append("\n");
		return regions;
		*/
	}

	private Map<String, RegionPartitioning> drtile(double maxWeight, JoinMatrix joinMatrix) throws TooSmallMaxWeightException{
		Map<String, RegionPartitioning> regPart = new HashMap<String, RegionPartitioning>();
		int coarsenedXTotalSize = _coarsener.getNumXCoarsenedPoints();
		int coarsenedYTotalSize = _coarsener.getNumYCoarsenedPoints();
		
		if(_cmode == COVERAGE_MODE.DENSE){
			// assumed non-monotonicity
			for(int halfPerimeter=2; halfPerimeter <= coarsenedXTotalSize + coarsenedYTotalSize; halfPerimeter++){
				for(int coarsenedRegionXSize = 1; coarsenedRegionXSize < halfPerimeter; coarsenedRegionXSize++){
					int coarsenedRegionYSize = halfPerimeter - coarsenedRegionXSize;
					for (int ci = 0; ci <= coarsenedXTotalSize - coarsenedRegionXSize; ci++){
						for(int cj = 0; cj <= coarsenedYTotalSize - coarsenedRegionYSize; cj++){
							int x1 = _coarsener.getOriginalXCoordinate(ci, false);
							int y1 = _coarsener.getOriginalYCoordinate(cj, false);
							int x2 = _coarsener.getOriginalXCoordinate(ci + coarsenedRegionXSize - 1, true);
							int y2 = _coarsener.getOriginalYCoordinate(cj + coarsenedRegionYSize - 1, true);
							Region region = new Region(x1, y1, x2, y2);
							partitionRegion(region, maxWeight, regPart);
						}
					}
				}
			}
		}else{
			for(Map.Entry<Integer, List<Region>> entry: _sccCoarsenedRegions.entrySet()){
				List<Region> regions = entry.getValue();
				for(Region region: regions){
					// regions with the same perimeter can be processed in any order
					partitionRegion(region, maxWeight, regPart);
				}
			}
		}
		
		return regPart;
	}

	private void partitionRegion(Region originalRegion, double maxWeight, Map<String, RegionPartitioning> regPart) throws TooSmallMaxWeightException{
		Region coarsenedRegion = originalRegion;
		// if the condition below is not fulfilled, coarsenedRegion is just an alias to originalRegion
		if(SystemParameters.COARSE_PRECOMPUTATION){
			coarsenedRegion = _coarsener.translateOriginalToCoarsenedRegion(originalRegion);
		}
		if(!_wp.isEmpty(coarsenedRegion)){
			if(_wp.getWeight(originalRegion) <= maxWeight){
				// I won't add it; for non-existing regions I assume num of rectangles 1 (if non-empty region), or 0(otherwise)
				// addRegion(region, regPart, wf);
			}else{
				addRegionWithPartitioning(originalRegion, maxWeight, regPart);
			}
		}
	}
	
	/*
	//region is Smallest Candidate Containing Region
	private void addRegion(Region region, Map<String, RegionPartitioning> regPart) {
		region.minimizeToNotEmpty(_wp);
		String regionHash = region.getHashString();
		RegionPartitioning rp = new RegionPartitioning(region);
		regPart.put(regionHash, rp);
	}
	*/

	private void addRegionWithPartitioning(Region region, double maxWeight, Map<String, RegionPartitioning> regPart) throws TooSmallMaxWeightException{
		int x1 = region.get_x1();
		int y1 = region.get_y1();
		int x2 = region.get_x2();
		int y2 = region.get_y2();

// version for regPart with original regions : does not create coarsenedRegion (instead use region)		
		Region coarsenedRegion = _coarsener.translateOriginalToCoarsenedRegion(region);
		String coarsenedRegionHash = coarsenedRegion.getHashString();
		
		// A1: Having the weight on the original region bigger than maxWeight
    	//    does not necessarily mean we will have two rectangles inside

		Region sccOriginal = null;
		if(_cmode == COVERAGE_MODE.DENSE){
// version for regPart with original regions			
//Region originalRegion = new Region(region);
//Region coarsenedRegion = _coarsener.translateOriginalToCoarsenedRegion(originalRegion);
			coarsenedRegion.minimizeToNotEmptyCoarsened(_wp, _coarsener); // in-place modification
			sccOriginal = _coarsener.translateCoarsenedToOriginalRegion(coarsenedRegion);
			if(!Region.equalPosition(region, sccOriginal)){
				// sccCoarsened is smaller than region; 
				//    it is already examined before

				// Only coarsened scc are stored; getNumJoiners and addRectangles methods to be changed

				/*
				 * When low-density regions are stored: O(n^4) times invoked
			String sccCoarsenedHash = sccCoarsened.getHashString();
			if(regPart.containsKey(sccCoarsenedHash)){
				// if there is an entry, that means more than a single rectangle; need to be copied
				RegionPartitioning sccRP = regPart.get(sccCoarsenedHash);
				regPart.put(regionHash, sccRP);
			}*/

				return;
			}
		}else if(_cmode == COVERAGE_MODE.SPARSE){
			// we generate only sccCoarsened regions in this mode
			sccOriginal = new Region(region);
		}
		
		// computing the minimum theoretical number of rectangles to cover the region
		// we are reporting on the level of coarsened grid cells (as a candidate grid cells has one artificially added output)
		double dblQtyMaxWeight = _wp.getWeight(sccOriginal) / maxWeight;
		//double dblQtyMaxWeight = _wp.getWeight(scc) / maxWeight;
		int qtyMaxWeigh = (int) dblQtyMaxWeight;
		if(qtyMaxWeigh != dblQtyMaxWeight){
			qtyMaxWeigh++;
		}
		
		if(dblQtyMaxWeight <= 1){
			// no need to put it in, as it need less than 1 rectangle
			return;
		}
		
		int cx1 = _coarsener.getCoarsenedXCoordinate(x1);
		int cy1 = _coarsener.getCoarsenedYCoordinate(y1);
		int cx2 = _coarsener.getCoarsenedXCoordinate(x2);
		int cy2 = _coarsener.getCoarsenedYCoordinate(y2);		
		
		// if we are of the unit size and bigger than weight, this is an exception
		if(isUnitSize(cx1, cy1, cx2, cy2)){
			_sb.append("\nImpossible to achieve maxWeight less than ").append(maxWeight).append(" with ").append(_coarsener).append("\n");
			throw new TooSmallMaxWeightException(maxWeight, _coarsener);
		}
		
		// I reach this point if
		//   a) region = sccCoarsened, meaning that there are elements on each boundary bucket
		//   b) its weight is higher than maxWeight
		
		int bestNumRectangles = Integer.MAX_VALUE;
		String bestFirstRegionHash = null;
		String bestSecondRegionHash = null;
		
		// try horizontal partitioning
		int lowerBound = cx1;
		int upperBound = cx2 - 1; // such that the second region has at least one coarsened point to cover
		if(_dmode == DIVISION_MODE.LOG){
			while(lowerBound <= upperBound){
				if(bestNumRectangles == qtyMaxWeigh){
					//achieved theoretical optimum, no need to try other combinations
					break;
				}
				
				int middleCoarsened = (lowerBound + upperBound) / 2;
				int middleOriginal = _coarsener.getOriginalXCoordinate(middleCoarsened, true);

				RegionHashNumRectangles first = createRegionHashNumCoarsened(x1, y1, middleOriginal, y2, cx1, cy1, middleCoarsened, cy2, regPart);			
				RegionHashNumRectangles second = createRegionHashNumCoarsened(middleOriginal+1, y1, x2, y2, middleCoarsened+1, cy1, cx2, cy2, regPart);

				int currentNumRectangles = first.getNumRectangles() + second.getNumRectangles();
				if(currentNumRectangles < bestNumRectangles){
					bestNumRectangles = currentNumRectangles;
					bestFirstRegionHash = first.getRegionHash();
					bestSecondRegionHash = second.getRegionHash();
				}

				// let's decide where to go
				// find the number of joiners of the first right neighbor which has different number of joiners than me
				int neighborNumRectangles = findFirstDifferentRightNeighborHor(x1, y1, x2, y2, cx1, cy1, cx2, cy2, 
						currentNumRectangles, middleCoarsened, upperBound, regPart);
				if(currentNumRectangles < neighborNumRectangles){
					upperBound = middleCoarsened - 1;
				}else if(currentNumRectangles > neighborNumRectangles){
					lowerBound = middleCoarsened + 1;
				}
			}
		}else if(_dmode == DIVISION_MODE.LINEAR){
			for(int ci = lowerBound; ci <= upperBound; ci++){
				if(bestNumRectangles == qtyMaxWeigh){
					//achieved theoretical optimum, no need to try other combinations
					break;
				}

				int i = _coarsener.getOriginalXCoordinate(ci, true);

				RegionHashNumRectangles first = createRegionHashNumCoarsened(x1, y1, i, y2, cx1, cy1, ci, cy2, regPart);			
				RegionHashNumRectangles second = createRegionHashNumCoarsened(i+1, y1, x2, y2, ci+1, cy1, cx2, cy2, regPart);

				// TODO To achieve better load balance, we could either
				//   a) rely on the right maxWeight (probably the right way to do it for large _j)
				//   b) if(firstNumRectangles == 1 && secondNumRectangles == 1) && (they have the same bestNumRectangles), 
				// pick one which minimizes Math.abs|weight(secondRegion) - weight(firstRegion)|			            
				int currentNumRectangles = first.getNumRectangles() + second.getNumRectangles();
				if(currentNumRectangles < bestNumRectangles){
					bestNumRectangles = currentNumRectangles;
					bestFirstRegionHash = first.getRegionHash();
					bestSecondRegionHash = second.getRegionHash();
				}
			}
		}
		
		// try vertical partitioning
		lowerBound = cy1;
		upperBound = cy2 - 1; // such that the second region has at least one coarsened point to cover
		if(_dmode == DIVISION_MODE.LOG){
			while(lowerBound <= upperBound){
				if(bestNumRectangles == qtyMaxWeigh){
					//achieved theoretical optimum, no need to try other combinations
					break;
				}
				
				int middleCoarsened = (lowerBound + upperBound) / 2;
				int middleOriginal = _coarsener.getOriginalYCoordinate(middleCoarsened, true);

				RegionHashNumRectangles first = createRegionHashNumCoarsened(x1, y1, x2, middleOriginal, cx1, cy1, cx2, middleCoarsened, regPart);			
				RegionHashNumRectangles second = createRegionHashNumCoarsened(x1, middleOriginal + 1, x2, y2, cx1, middleCoarsened + 1, cx2, cy2, regPart);

				int currentNumRectangles = first.getNumRectangles() + second.getNumRectangles();
				if(currentNumRectangles < bestNumRectangles){
					bestNumRectangles = currentNumRectangles;
					bestFirstRegionHash = first.getRegionHash();
					bestSecondRegionHash = second.getRegionHash();
				}

				// let's decide where to go
				// find the number of joiners of the first right neighbor which has different number of joiners than me
				int neighborNumRectangles = findFirstDifferentRightNeighborVer(x1, y1, x2, y2, cx1, cy1, cx2, cy2, 
						currentNumRectangles, middleCoarsened, upperBound, regPart);
				if(currentNumRectangles < neighborNumRectangles){
					upperBound = middleCoarsened - 1;
				}else if(currentNumRectangles > neighborNumRectangles){
					lowerBound = middleCoarsened + 1;
				}
			}
		}else if(_dmode == DIVISION_MODE.LINEAR){
			for(int ci = lowerBound; ci <= upperBound; ci++){
				if(bestNumRectangles == qtyMaxWeigh){
					//achieved theoretical optimum, no need to try other combinations
					break;
				}

				int i = _coarsener.getOriginalYCoordinate(ci, true);

				RegionHashNumRectangles first = createRegionHashNumCoarsened(x1, y1, x2, i, cx1, cy1, cx2, ci, regPart);			
				RegionHashNumRectangles second = createRegionHashNumCoarsened(x1, i+1, x2, y2, cx1, ci+1, cx2, cy2, regPart);
				
				// TODO To achieve better load balance, we could either
				//   a) rely on the right maxWeight (probably the right way to do it for large _j)
				//   b) if(firstNumRectangles == 1 && secondNumRectangles == 1) && (they have the same bestNumRectangles), 
				// pick one which minimizes Math.abs|weight(secondRegion) - weight(firstRegion)|
				int currentNumRectangles = first.getNumRectangles() + second.getNumRectangles();
				if(currentNumRectangles < bestNumRectangles){
					bestNumRectangles = currentNumRectangles;
					bestFirstRegionHash = first.getRegionHash();
					bestSecondRegionHash = second.getRegionHash();
				}
			}
		}
		
		if(bestNumRectangles < 2){
			throw new RuntimeException("Developer error!");
		}
		
		RegionPartitioning rp = new RegionPartitioning(region, bestFirstRegionHash, bestSecondRegionHash, bestNumRectangles);
		regPart.put(coarsenedRegionHash, rp);
	}

	// upperBound is the last possible position of the divisor, such that the second part has at least one coarsened point
	// return numOfJoiners
	private int findFirstDifferentRightNeighborHor(int x1, int y1, int x2, int y2, int cx1, int cy1, int cx2, int cy2, 
			int currentNumRectangles, int currentMiddle, int upperBound, Map<String, RegionPartitioning> regPart) {
		int rightNeighbor = currentMiddle + 1;
		while (rightNeighbor <= upperBound){
			int rightOriginal = _coarsener.getOriginalXCoordinate(rightNeighbor, true);
			RegionHashNumRectangles first = createRegionHashNumCoarsened(x1, y1, rightOriginal, y2, cx1, cy1, rightNeighbor, cy2, regPart);			
			RegionHashNumRectangles second = createRegionHashNumCoarsened(rightOriginal+1, y1, x2, y2, rightNeighbor+1, cy1, cx2, cy2, regPart);
			int rightNumRectangles = first.getNumRectangles() + second.getNumRectangles();
			if(rightNumRectangles != currentNumRectangles){
				return rightNumRectangles;
			}
			rightNeighbor++;
		}
		
		//if all of them are the same, I return infinity, such that the binary search goes on the left side
		return Integer.MAX_VALUE;
	}

	// upperBound is the last possible position of the divisor, such that the second part has at least one coarsened point
	// return numOfJoiners
	private int findFirstDifferentRightNeighborVer(int x1, int y1, int x2, int y2, int cx1, int cy1, int cx2, int cy2, 
			int currentNumRectangles, int currentMiddle, int upperBound, Map<String, RegionPartitioning> regPart) {
		int rightNeighbor = currentMiddle + 1;
		while (rightNeighbor <= upperBound){
			int rightOriginal = _coarsener.getOriginalYCoordinate(rightNeighbor, true);
			RegionHashNumRectangles first = createRegionHashNumCoarsened(x1, y1, x2, rightOriginal, cx1, cy1, cx2, rightNeighbor, regPart);			
			RegionHashNumRectangles second = createRegionHashNumCoarsened(x1, rightOriginal + 1, x2, y2, cx1, rightNeighbor + 1, cx2, cy2, regPart);
			int rightNumRectangles = first.getNumRectangles() + second.getNumRectangles();
			if(rightNumRectangles != currentNumRectangles){
				return rightNumRectangles;
			}
			rightNeighbor++;
		}
		
		//if all of them are the same, I return infinity, such that the binary search goes on the left side
		return Integer.MAX_VALUE;
	}
	 
	/*
    // invoked many times
	private RegionHashNumRectangles createRegionHashNum(int x1, int y1, int x2, int y2, Map<String, RegionPartitioning> regPart) {
		Region region = new Region(x1, y1, x2, y2);
		String regionHash = region.getHashString();
		int numRectangles = getNumJoiners(regPart, region, regionHash);
		return new RegionHashNumRectangles(regionHash, numRectangles);
	}
	*/

    // invoked many times; more efficient version; 
	// for the new version of the code, first four arguments are not necessary (if I remove them (and the unnecessary commands), no performance improvement)
	private RegionHashNumRectangles createRegionHashNumCoarsened(int x1, int y1, int x2, int y2,
			int cx1, int cy1, int cx2, int cy2, Map<String, RegionPartitioning> regPart) {
		Region regionCoarsened = new Region(cx1, cy1, cx2, cy2);
		String coarsenedRegionHash = regionCoarsened.getHashString();
		int numRectangles = getNumJoinersCoarsened(regPart, regionCoarsened);
		return new RegionHashNumRectangles(coarsenedRegionHash, numRectangles);
// version for regPart with original regions
//Region regionOriginal = new Region(x1, y1, x2, y2);
//Region regionCoarsened = new Region(cx1, cy1, cx2, cy2);
//String originalRegionHash = regionOriginal.getHashString();
//int numRectangles = getNumJoinersCoarsened(regPart, regionCoarsened);
//return new RegionHashNumRectangles(originalRegionHash, numRectangles);
	}
	
	
	public String toString(){
		return "BSPAlgorithm with " + _coarsener;
	}
	
	// invoked once per binary search iteration
	private int getNumJoiners(Map<String, RegionPartitioning> regPart, JoinMatrix joinMatrix) {
		String coarsenedMatrixHash = getCoarsenedRegionMatrixHash(regPart, joinMatrix);
		Region coarsenedRegion = new Region(coarsenedMatrixHash);
		return getNumJoinersCoarsened(regPart, coarsenedRegion);
// version for regPart with original regions
//String matrixHash = getRegionMatrixHash(regPart, joinMatrix);
//Region originalRegion = new Region(matrixHash);
//Region coarsenedRegion = _coarsener.translateOriginalToCoarsenedRegion(originalRegion);
//return getNumJoinersCoarsened(regPart, coarsenedRegion);
	}
	
	/*
	// invoked many times 
	private int getNumJoiners(Map<String, RegionPartitioning> regPart, Region region, String regionHash) {
		// Only coarsened scc are stored
		Region scc = new Region(regionHash);
		scc.minimizeToNotEmpty(_wp);	
		Region sccCoarsened = getEnclosingRegion(scc);
		String sccCoarsenedHash = sccCoarsened.getHashString();
		if(regPart.containsKey(sccCoarsenedHash)){
			return regPart.get(sccCoarsenedHash).getNumRectangles();	
		}else{
			// if we are asking for it, it must have at least one rectangle
			return 1;
		}
	}
	*/

	// this should be where most of the processing time is spent (call from createRegionHashNumCoarsened, which is called from findFirstDifferentRightNeighborHor)
	// more efficient version: 
	private int getNumJoinersCoarsened(Map<String, RegionPartitioning> regPart, Region regionCoarsened) {
		regionCoarsened.minimizeToNotEmptyCoarsened(_wp, _coarsener); // in-place modification

		String regionCoarsenedHash = regionCoarsened.getHashString();
		if(regPart.containsKey(regionCoarsenedHash)){
			return regPart.get(regionCoarsenedHash).getNumRectangles();	
		}else{
			// if we are asking for it, it must have at least one rectangle
			return 1;
		}

// version for regPart with original regions		
// Fixed: TODO not a bottleneck: We could make everything by default coarsened and avoid translations 
//		Region sccOriginal = _coarsener.translateCoarsenedToOriginalRegion(regionCoarsened);
//		
//		String sccOriginalHash = sccOriginal.getHashString();
//		if(regPart.containsKey(sccOriginalHash)){
//			return regPart.get(sccOriginalHash).getNumRectangles();	
//		}else{
//			// if we are asking for it, it must have at least one rectangle
//			return 1;
//		}
		
		/* When low-density regions are stored: O(n^4) times invoked
		 if(regPart.containsKey(regionHash)){
			return regPart.get(regionHash).getNumRectangles();	
		}else{
			// A1: regions with more than 1 rectangles are explicitly present
			
			// if region was not passed, we instantiate it from hash
			if(region == null){
				region = new Region(regionHash);
			}
			
			if(wf.isNotEmpty(region)){
				return 1;
			}else{
				return 0;
			}
		}
		 */
	}
	
	
	// invoked once per binary search iteration
	private List<Region> getRegions(Map<String, RegionPartitioning> regPart, JoinMatrix joinMatrix) {
		// Only coarsened scc are stored
		String coarsenedMatrixHash = getCoarsenedRegionMatrixHash(regPart, joinMatrix);
		Region coarsenedRegion = new Region(coarsenedMatrixHash);
		coarsenedRegion.minimizeToNotEmptyCoarsened(_wp, _coarsener); // in-place modification
		String coarsenedHash = coarsenedRegion.getHashString();
		if(regPart.containsKey(coarsenedHash)){
			return regPart.get(coarsenedHash).getRectangles(regPart, this);	
		}else{
			// weight(matrixRegion) < maxWeight; that's why it was not inserted
			List<Region> regions = new ArrayList<Region>();
			// we are reporting on the level of coarsened grid cells (as a candidate grid cells has one artificially added output)
			Region sccOriginal = _coarsener.translateCoarsenedToOriginalRegion(coarsenedRegion);
			regions.add(sccOriginal);
			// regions.add(scc);
			return regions;
		}

// version for regPart with original regions		
//		String matrixHash = getRegionMatrixHash(regPart, joinMatrix);
//		Region originalRegion = new Region(matrixHash);
//		Region coarsenedRegion = _coarsener.translateOriginalToCoarsenedRegion(originalRegion);
//		coarsenedRegion.minimizeToNotEmptyCoarsened(_wp, _coarsener); // in-place modification
//		Region sccOriginal = _coarsener.translateCoarsenedToOriginalRegion(coarsenedRegion);
//		String sccOriginalHash = sccOriginal.getHashString();
//		if(regPart.containsKey(sccOriginalHash)){
//			return regPart.get(sccOriginalHash).getRectangles(regPart, this);	
//		}else{
//			// weight(matrixRegion) < maxWeight; that's why it was not inserted
//			List<Region> regions = new ArrayList<Region>();
//			// we are reporting on the level of coarsened grid cells (as a candidate grid cells has one artificially added output)
//			regions.add(sccOriginal);
//			// regions.add(scc);
//			return regions;
//		}
		
		/*
		 * Old way
		originalRegion.minimizeToNotEmpty(_wp);
		Region sccCoarsened = getEnclosingRegion(originalRegion);
		*/
	}

// version for regPart with original regions	
//	private String getRegionMatrixHash(Map<String, RegionPartitioning> regPart, JoinMatrix joinMatrix) {
//		int xSize = joinMatrix.getXSize() - 1;
//		int ySize = joinMatrix.getYSize() - 1;
//		Region region = new Region(0, 0, xSize, ySize);
//		return region.getHashString();
//	}
	
	private String getCoarsenedRegionMatrixHash(Map<String, RegionPartitioning> regPart, JoinMatrix joinMatrix) {
		// int xSize = joinMatrix.getXSize() - 1;
		// int ySize = joinMatrix.getYSize() - 1;
		int xSize = _coarsener.getNumXCoarsenedPoints() - 1;
		int ySize = _coarsener.getNumYCoarsenedPoints() - 1;
		Region region = new Region(0, 0, xSize, ySize);
		return region.getHashString();
	}

	
	// originally in coarsener
	/*
	 * We are using this method as we want to reduce the number of rectangles which we take into account for DP algorithm
	 *    Namely, a regions either contains the grid cell as a whole, or not at all.
	 */
	public Region getEnclosingRegion(Region scc) {
		int cx1 = _coarsener.getCoarsenedXCoordinate(scc.get_x1());
		int cy1 = _coarsener.getCoarsenedYCoordinate(scc.get_y1());
		int cx2 = _coarsener.getCoarsenedXCoordinate(scc.get_x2());
		int cy2 = _coarsener.getCoarsenedYCoordinate(scc.get_y2());
		
		int x1 = _coarsener.getOriginalXCoordinate(cx1, false);
		int y1 = _coarsener.getOriginalYCoordinate(cy1, false);
		int x2 = _coarsener.getOriginalXCoordinate(cx2, true);
		int y2 = _coarsener.getOriginalYCoordinate(cy2, true);
		
		return new Region(x1, y1, x2, y2);
	}

	// coarsened coordinates
	public boolean isUnitSize(int cx1, int cy1, int cx2, int cy2) {
		return cx1 == cx2 && cy1 == cy2;
	}	

	// monotonic optimization not worthed - similar as for getCandidateRoundedCells
	public List<Region> getGridCells() {
		List<Region> regions = new ArrayList<Region>();
		for(int i = 0; i < _coarsener.getNumXCoarsenedPoints(); i++){
			for(int j = 0; j < _coarsener.getNumYCoarsenedPoints(); j++){
				int x1 = _coarsener.getOriginalXCoordinate(i, false);
				int y1 = _coarsener.getOriginalYCoordinate(j, false);
				int x2 = _coarsener.getOriginalXCoordinate(i, true);
				int y2 = _coarsener.getOriginalYCoordinate(j, true);
				Region region = new Region(x1, y1, x2, y2);
				regions.add(region);				
			}
		}
		return regions;
	}
	// end of coarsener
	
	// Take advantage of sparseness
	// monotonic optimization not worthed - similar as for getCandidateRoundedCells
	private void precomputeSegmentExtremes(JoinMatrix joinMatrix){
		_segmentExtremes = new HashMap<Segment, ExtremePositions>();
		
		// first we compute the extreme points per line
		//    lines with no candidate grid cells are not added 
		for(int i=0; i < _coarsener.getNumXCoarsenedPoints(); i++){
			int x1 = _coarsener.getOriginalXCoordinate(i, false);
			int x2 = _coarsener.getOriginalXCoordinate(i, true);
			Segment segment = new Segment(i, i);
			boolean isSetMostLeft = false;
			for(int j = 0; j < _coarsener.getNumYCoarsenedPoints(); j++){
				int y1 = _coarsener.getOriginalYCoordinate(j, false);
				int y2 = _coarsener.getOriginalYCoordinate(j, true);
				
				Region region = new Region(x1, y1, x2, y2);
				if(MyUtilities.isCandidateRegion(joinMatrix, region, _cp, _map)){
					if(!isSetMostLeft){
						ExtremePositions ep = new ExtremePositions(j, j);
						_segmentExtremes.put(segment, ep);
						isSetMostLeft = true;
					}else{
						// ep already exists
						ExtremePositions ep = _segmentExtremes.get(segment);
						ep.setMostRight(j); // update the last position (i) with value j in place
					}
				}
			}
		}
		
		// height is total number of points of the region per dimension X
		for(int height = 2; height <= _coarsener.getNumXCoarsenedPoints(); height++){
			for(int i = 0; i <= _coarsener.getNumXCoarsenedPoints() - height; i++){
				int cx1 = i;
				int cx2 = i + height - 1;
				int upDivider = i; // arbitrary point in between is a divider
				int lowDivider = i + 1;
				
				Segment s1 = new Segment(cx1, upDivider);
				Segment s2 = new Segment(lowDivider, cx2);
				ExtremePositions ep1 = _segmentExtremes.get(s1);
				if(ep1 == null){
					continue; // do not analyze this segment, as the topLine does not exist
				}
				ExtremePositions ep2 = _segmentExtremes.get(s2);
				if (ep2 == null){
					ep2 = findCandSegment(s2);
					// if it's still null, s2 does not exist, so we do not analyze this segment at all
					if(ep2 == null){
						continue;
					}
				}
				int mostLeft = MyUtilities.getMin(ep1.getMostLeft(), ep2.getMostLeft());
				int mostRight = MyUtilities.getMax(ep1.getMostRight(), ep2.getMostRight());
				
				// adding multi-line segments
				Segment segment = new Segment(cx1, cx2);
				ExtremePositions ep = new ExtremePositions(mostLeft, mostRight);
				_segmentExtremes.put(segment, ep);
			}
		}
	}
	
	/*
	 * Given a horizontal segment, peel off top non-candidate lines
	 * Could be done in O(logB) time, where B is the number of buckets
	 *        but it's really not on the critical path 
	 *        (precomputation is reduced from 
	 *             O(B^3) (which is by the way never the case to be so bad) 
	 *             to O(B^2 logB)) 
	 */
	private ExtremePositions findCandSegment(Segment segment) {
		int lowerPos = segment.getLowerPos();
		int upperPos = segment.getUpperPos();
		
		//check upperPos
		Segment s2 = new Segment(upperPos, upperPos);
		if(!_segmentExtremes.containsKey(s2)){
			// the original region cannot exist, and thus cannot be divided into two segments, 
			//   The segment passed to this method is the upper one
			return null;
		}
		
		// move lowerPos
		Segment s1 = new Segment(lowerPos, lowerPos);
		while(!_segmentExtremes.containsKey(s1)){
			lowerPos++;
			s1 = new Segment(lowerPos, lowerPos);
		}
		
		return _segmentExtremes.get(new Segment(lowerPos, upperPos));
	}

	// SortedMap<Perimeter, Region>
	// Perimeter is in terms of coarsened matrix, Regions are in terms of original Matrix
	// no performance improvement if I keep coarsenedRegions instead
	private SortedMap<Integer, List<Region>> generateSCCCoarsenedRegions() {
		SortedMap<Integer, List<Region>> result = new TreeMap<Integer, List<Region>>();
		
		// two outer loops go over all the horizontal segments
		// height is total number of points of the region per dimension X
		for(int height = 1; height <= _coarsener.getNumXCoarsenedPoints(); height++){
			for(int i = 0; i <= _coarsener.getNumXCoarsenedPoints() - height; i++){
				int cx1 = i;
				int cx2 = i + height - 1;
				
				Segment segment = new Segment(cx1, cx2);
				ExtremePositions ep = _segmentExtremes.get(segment);
				if(ep == null){
					// do not analyze this segment, as it does not exist in the _segmentExtremes
					//   this means that either topLine or bottomLine does not exist
					continue;
				}
				int mostLeft = ep.getMostLeft();
				int mostRight = ep.getMostRight();
				
				Segment topLine = new Segment(cx1, cx1);
				ExtremePositions epTop = _segmentExtremes.get(topLine);
				int topLineMostRight = epTop.getMostRight();
				Segment bottomLine = new Segment(cx2, cx2);
				ExtremePositions epBottom = _segmentExtremes.get(bottomLine);
				int bottomLineMostLeft = epBottom.getMostLeft();
				
				for(int p = mostLeft; p <= topLineMostRight; p++){
					for(int q = MyUtilities.getMax(p, bottomLineMostLeft); q <= mostRight; q++){
						// cx1, cx2 already exists, cy1 = p, cy2 = q
						Region coarsened = new Region(cx1, p, cx2, q);
						int halfPerimeter = coarsened.getHalfPerimeter();
						
						int x1 = _coarsener.getOriginalXCoordinate(cx1, false);
						int y1 = _coarsener.getOriginalYCoordinate(p, false);
						int x2 = _coarsener.getOriginalXCoordinate(cx2, true);
						int y2 = _coarsener.getOriginalYCoordinate(q, true);
						Region region = new Region(x1, y1, x2, y2);
						
						addToCollection(result, halfPerimeter, region);
					}
				}
			}
		}
				
		return result;
	}
	
	private void addToCollection(SortedMap<Integer, List<Region>> collection, int halfPerimeter, Region region) {
		List<Region> regions = null;
		if(collection.containsKey(halfPerimeter)){
			regions = collection.get(halfPerimeter);
		}else{
			regions = new ArrayList<Region>();
			collection.put(halfPerimeter, regions);
		}
		regions.add(region);
	}
	
	// end of sparseness

	// hashes are for coarsened regions, but the regions are actually original
	public class RegionPartitioning{
		private Region _region; 
		
		private String _bestFirstSubregionHash = null;
		private String _bestSecondSubregionHash = null;
		private int _numRectangles; // a rectangle is of maximum weight maxWeight
		
		// weight of a region less than maxWeight
		public RegionPartitioning(Region region){
			_region = region;
			_numRectangles = 1;
		}
		
		// weight of a region bigger than maxWeight; it is partitioned into 2 subregions, such that the total numRectangles is minimized
		public RegionPartitioning(Region region, String bestFirstSubregionHash, String bestSecondSubregionHash, int numRectangles){
			_region = region;
			_bestFirstSubregionHash = bestFirstSubregionHash;
			_bestSecondSubregionHash = bestSecondSubregionHash;
			_numRectangles = numRectangles;
		}
		
		public Region getRegion(){
			return _region;
		}
		
		public int getNumRectangles() {
			return _numRectangles;
		}
		
		public String getBestFirstSubregionHash(){
			return _bestFirstSubregionHash;
		}
		
		public String getBestSecondSubregionHash(){
			return _bestSecondSubregionHash;
		}
		
		// invoked once per binary search iteration
		public List<Region> getRectangles(Map<String, RegionPartitioning> regPart, BSPAlgorithm bsp){
			List<Region> allRegions = new ArrayList<Region>();
			
			addRectangles(allRegions, _bestFirstSubregionHash, regPart, bsp);
			addRectangles(allRegions, _bestSecondSubregionHash, regPart, bsp);
			
			return allRegions;
		}
		
		//allRegions appended with regions
		private void addRectangles(List<Region> allRegions, String coarsenedRegionHash, Map<String, RegionPartitioning> regPart, BSPAlgorithm bsp) {
			Region coarsenedRegion = new Region(coarsenedRegionHash);
			int numRectangles = bsp.getNumJoinersCoarsened(regPart, coarsenedRegion);
			coarsenedRegion.minimizeToNotEmptyCoarsened(_wp, _coarsener); // in-place modification
			
			// we ignore regions with 0 rectangles; should not appear anyway (look at A1 assumption)
			if(numRectangles == 1){
				//we are reporting on the level of coarsened grid cells (as a candidate grid cells has one artificially added output)
				Region sccOriginal = _coarsener.translateCoarsenedToOriginalRegion(coarsenedRegion);
				allRegions.add(sccOriginal);
				
				//allRegions.add(scc);
			}else if (numRectangles > 1){
				//recursive invocation
				
				// Only coarsened scc are stored, because of candidate grid cells which contain only 1 element per grid cell
				String coarsenedHash = coarsenedRegion.getHashString();
				allRegions.addAll(regPart.get(coarsenedHash).getRectangles(regPart, bsp));
				
				/* When low-density regions are stored: (not on critical path, invoked only at the end)
				  allRegions.addAll(regPart.get(regionHash).getRectangles(regPart, wp));
				 */
			}
		}

// version for regPart with original regions
//		private void addRectangles(List<Region> allRegions, String regionHash, Map<String, RegionPartitioning> regPart, BSPAlgorithm bsp) {
//			Region originalRegion = new Region(regionHash);
//			Region coarsenedRegion = _coarsener.translateOriginalToCoarsenedRegion(originalRegion);
//			int numRectangles = bsp.getNumJoinersCoarsened(regPart, coarsenedRegion);
//			coarsenedRegion.minimizeToNotEmptyCoarsened(_wp, _coarsener); // in-place modification
//			Region sccOriginal = _coarsener.translateCoarsenedToOriginalRegion(coarsenedRegion);
//			
//			// we ignore regions with 0 rectangles; should not appear anyway (look at A1 assumption)
//			if(numRectangles == 1){
//				//we are reporting on the level of coarsened grid cells (as a candidate grid cells has one artificially added output)
//				allRegions.add(sccOriginal);
//				
//				//allRegions.add(scc);
//			}else if (numRectangles > 1){
//				//recursive invocation
//				
//				// Only coarsened scc are stored, because of candidate grid cells which contain only 1 element per grid cell
//				String sccOriginalHash = sccOriginal.getHashString();
//				allRegions.addAll(regPart.get(sccOriginalHash).getRectangles(regPart, bsp));
//				
//				/* When low-density regions are stored: (not on critical path, invoked only at the end)
//				  allRegions.addAll(regPart.get(regionHash).getRectangles(regPart, wp));
//				 */
//			}
//		}
	}
	
	private class Segment{
		// coarsened points
		private int _lowerPos, _upperPos;
		
		public Segment(int lowerPos, int upperPos){
			_lowerPos = lowerPos;
			_upperPos = upperPos;
		}
		
		public Segment(String hash){
			String[] parts = hash.split("-");
			_lowerPos = Integer.parseInt(new String(parts[0]));
			_upperPos = Integer.parseInt(new String(parts[1]));
		}
		
		public int getLowerPos(){
			return _lowerPos;
		}
		
		public int getUpperPos(){
			return _upperPos;
		}
		
		public void setLowerPos(int lowerPos){
			_lowerPos = lowerPos;
		}
		
		public void setUpperPos(int upperPos){
			_upperPos = upperPos;
		}
		
		@Override
		public String toString(){
			return "" + _lowerPos + "-" + _upperPos; 
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + _lowerPos;
			result = prime * result + _upperPos;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Segment other = (Segment) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (_lowerPos != other._lowerPos)
				return false;
			if (_upperPos != other._upperPos)
				return false;
			return true;
		}

		private BSPAlgorithm getOuterType() {
			return BSPAlgorithm.this;
		}
	}
	
	private class RegionHashNumRectangles{
		private String _regionHash;
		private int _numRectangles;
		
		public RegionHashNumRectangles(String regionHash, int numRectangles){
			_regionHash = regionHash;
			_numRectangles = numRectangles;
		}
		
		public String getRegionHash(){
			return _regionHash;
		}
		
		public int getNumRectangles(){
			return _numRectangles;
		}
		
		public void setRegionHash(String regionHash){
			_regionHash = regionHash;
		}
		
		public void setNumRectangles(int numRectangles){
			_numRectangles = numRectangles;
		}
	}

	@Override
	public String getShortName() {
		String shortName = "bsp_";
		if(_coarsener instanceof InputShallowCoarsener){
			shortName += "i";
		}else if (_coarsener instanceof InputOutputShallowCoarsener){
			shortName += "io";
		}else if (_coarsener instanceof OutputShallowCoarsener){
			shortName += "o";
		}else{
			throw new RuntimeException("Unsupported coarsener " + _coarsener.toString());
		}
		return shortName;
	}
}