package ch.epfl.data.plan_runner.ewh.main;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.conversion.DateIntegerConversion;
import ch.epfl.data.plan_runner.conversion.IntegerConversion;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.ewh.algorithms.BSPAlgorithm;
import ch.epfl.data.plan_runner.ewh.algorithms.InputOutputShallowCoarsener;
import ch.epfl.data.plan_runner.ewh.algorithms.InputShallowCoarsener;
import ch.epfl.data.plan_runner.ewh.algorithms.OkcanExactInputAlgorithm;
import ch.epfl.data.plan_runner.ewh.algorithms.OkcanExactOutputAlgorithm;
import ch.epfl.data.plan_runner.ewh.algorithms.ShallowCoarsener;
import ch.epfl.data.plan_runner.ewh.algorithms.TilingAlgorithm;
import ch.epfl.data.plan_runner.ewh.algorithms.WeightPrecomputation;
import ch.epfl.data.plan_runner.ewh.algorithms.optimality.MaxAvgOptimality;
import ch.epfl.data.plan_runner.ewh.algorithms.optimality.OptimalityMetricInterface;
import ch.epfl.data.plan_runner.ewh.algorithms.optimality.WeightFunction;
import ch.epfl.data.plan_runner.ewh.data_structures.FrequencyPosition;
import ch.epfl.data.plan_runner.ewh.data_structures.JoinMatrix;
import ch.epfl.data.plan_runner.ewh.data_structures.KeyRegion;
import ch.epfl.data.plan_runner.ewh.data_structures.Region;
import ch.epfl.data.plan_runner.ewh.data_structures.UJMPAdapterByteMatrix;
import ch.epfl.data.plan_runner.ewh.visualize.UJMPVisualizer;
import ch.epfl.data.plan_runner.ewh.visualize.VisualizerInterface;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.utilities.DeepCopy;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class PushStatisticCollector {
	private static class Hyracks implements PLCQueryPlan {
		private IntegerConversion _ic = new IntegerConversion();

		private Map _map;

		private List<Integer> _firstJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _firstProject = new ProjectOperator(
				new int[] { 0 });
		private List<Integer> _secondJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _secondProject = new ProjectOperator(
				new int[] { 0 });

		public Hyracks(Map map) {
			_map = map;
		}

		@Override
		public JoinMatrix generateMatrix() {
			Collections.sort(_firstJoinKeys);
			Collections.sort(_secondJoinKeys);

			ComparisonPredicate<Integer> comparison = new ComparisonPredicate<Integer>(
					ComparisonPredicate.EQUAL_OP);

			JoinMatrix joinMatrix = new UJMPAdapterByteMatrix(
					_firstJoinKeys.size(), _secondJoinKeys.size(), _map,
					comparison, _ic);
			fillMatrix(joinMatrix, _firstJoinKeys, _secondJoinKeys, comparison);
			return joinMatrix;
		}

		@Override
		public NumericConversion getWrapper() {
			return _ic;
		}

		@Override
		public void processTuple(List<String> tuple, int relationNumber) {
			if (relationNumber == 0) {
				// R update
				appendKeys(_firstJoinKeys, _firstProject, tuple, _ic);
			} else if (relationNumber == 1) {
				// S update
				appendKeys(_secondJoinKeys, _secondProject, tuple, _ic);
			} else {
				throw new RuntimeException("Unknown relationNumber "
						+ relationNumber);
			}
		}
	}

	private static interface PLCQueryPlan {
		public JoinMatrix generateMatrix();

		public NumericConversion getWrapper();

		public void processTuple(List<String> tuple, int relationNumber);
	}

	private static class ThetaLineitemSelfJoin implements PLCQueryPlan {
		private DateIntegerConversion _dateIntConv = new DateIntegerConversion();

		private Map _map;

		private List<Integer> _firstJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _firstProject = new ProjectOperator(
				new int[] { 0 });
		private List<Integer> _secondJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _secondProject = new ProjectOperator(
				new int[] { 0 });

		public ThetaLineitemSelfJoin(Map map) {
			_map = map;
		}

		@Override
		public JoinMatrix generateMatrix() {
			Collections.sort(_firstJoinKeys);
			Collections.sort(_secondJoinKeys);

			ComparisonPredicate<Integer> comparison = new ComparisonPredicate<Integer>(
					ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, 1,
					_dateIntConv);

			JoinMatrix joinMatrix = new UJMPAdapterByteMatrix(
					_firstJoinKeys.size(), _secondJoinKeys.size(), _map,
					comparison, _dateIntConv);
			fillMatrix(joinMatrix, _firstJoinKeys, _secondJoinKeys, comparison);
			return joinMatrix;
		}

		@Override
		public NumericConversion getWrapper() {
			return _dateIntConv;
		}

		@Override
		public void processTuple(List<String> tuple, int relationNumber) {
			if (relationNumber == 0) {
				// R update
				appendKeys(_firstJoinKeys, _firstProject, tuple, _dateIntConv);
			} else if (relationNumber == 1) {
				// S update
				appendKeys(_secondJoinKeys, _secondProject, tuple, _dateIntConv);
			} else {
				throw new RuntimeException("Unknown relationNumber "
						+ relationNumber);
			}
		}
	}

	private static class ThetaLineitemSelfJoinInputDominated implements
			PLCQueryPlan {
		private IntegerConversion _ic = new IntegerConversion();

		private Map _map;

		private List<Integer> _firstJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _firstProject = new ProjectOperator(
				new int[] { 5 });
		private List<Integer> _secondJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _secondProject = new ProjectOperator(
				new int[] { 5 });

		public ThetaLineitemSelfJoinInputDominated(Map map) {
			_map = map;
		}

		@Override
		public JoinMatrix generateMatrix() {
			Collections.sort(_firstJoinKeys);
			Collections.sort(_secondJoinKeys);

			ComparisonPredicate<Integer> comparison = new ComparisonPredicate<Integer>(
					ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP, 1, _ic);

			JoinMatrix joinMatrix = new UJMPAdapterByteMatrix(
					_firstJoinKeys.size(), _secondJoinKeys.size(), _map,
					comparison, _ic);
			fillMatrix(joinMatrix, _firstJoinKeys, _secondJoinKeys, comparison);
			return joinMatrix;
		}

		@Override
		public NumericConversion getWrapper() {
			return _ic;
		}

		@Override
		public void processTuple(List<String> tuple, int relationNumber) {
			if (relationNumber == 0) {
				// R update
				appendKeys(_firstJoinKeys, _firstProject, tuple, _ic);
			} else if (relationNumber == 1) {
				// S update
				appendKeys(_secondJoinKeys, _secondProject, tuple, _ic);
			} else {
				throw new RuntimeException("Unknown relationNumber "
						+ relationNumber);
			}
		}
	}

	private static class ThetaTPCH5_R_N_S_L implements PLCQueryPlan {
		private IntegerConversion _ic = new IntegerConversion();

		private Map _map;

		private List<Integer> _firstJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _firstProject = new ProjectOperator(
				new int[] { 2 });
		private List<Integer> _secondJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _secondProject = new ProjectOperator(
				new int[] { 1 });

		public ThetaTPCH5_R_N_S_L(Map map) {
			_map = map;
		}

		@Override
		public JoinMatrix generateMatrix() {
			Collections.sort(_firstJoinKeys);
			Collections.sort(_secondJoinKeys);

			ComparisonPredicate<Integer> comparison = new ComparisonPredicate<Integer>(
					ComparisonPredicate.EQUAL_OP);

			JoinMatrix joinMatrix = new UJMPAdapterByteMatrix(
					_firstJoinKeys.size(), _secondJoinKeys.size(), _map,
					comparison, _ic);
			fillMatrix(joinMatrix, _firstJoinKeys, _secondJoinKeys, comparison);
			return joinMatrix;
		}

		@Override
		public NumericConversion getWrapper() {
			return _ic;
		}

		@Override
		public void processTuple(List<String> tuple, int relationNumber) {
			if (relationNumber == 0) {
				// R update
				appendKeys(_firstJoinKeys, _firstProject, tuple, _ic);
			} else if (relationNumber == 1) {
				// S update
				appendKeys(_secondJoinKeys, _secondProject, tuple, _ic);
			} else {
				throw new RuntimeException("Unknown relationNumber "
						+ relationNumber);
			}
		}
	}

	private static class ThetaTPCH7_L_S_N1 implements PLCQueryPlan {
		private IntegerConversion _ic = new IntegerConversion();

		private Map _map;

		private List<Integer> _firstJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _firstProject = new ProjectOperator(
				new int[] { 2 });
		private List<Integer> _secondJoinKeys = new ArrayList<Integer>();
		private ProjectOperator _secondProject = new ProjectOperator(
				new int[] { 0 });

		public ThetaTPCH7_L_S_N1(Map map) {
			_map = map;
		}

		@Override
		public JoinMatrix generateMatrix() {
			Collections.sort(_firstJoinKeys);
			Collections.sort(_secondJoinKeys);

			ComparisonPredicate<Integer> comparison = new ComparisonPredicate<Integer>(
					ComparisonPredicate.EQUAL_OP);

			JoinMatrix joinMatrix = new UJMPAdapterByteMatrix(
					_firstJoinKeys.size(), _secondJoinKeys.size(), _map,
					comparison, _ic);
			fillMatrix(joinMatrix, _firstJoinKeys, _secondJoinKeys, comparison);
			return joinMatrix;
		}

		@Override
		public NumericConversion getWrapper() {
			return _ic;
		}

		@Override
		public void processTuple(List<String> tuple, int relationNumber) {
			if (relationNumber == 0) {
				// R update
				appendKeys(_firstJoinKeys, _firstProject, tuple, _ic);
			} else if (relationNumber == 1) {
				// S update
				appendKeys(_secondJoinKeys, _secondProject, tuple, _ic);
			} else {
				throw new RuntimeException("Unknown relationNumber "
						+ relationNumber);
			}
		}
	}

	private static <T> void appendKeys(List<T> keys, ProjectOperator project,
			List<String> tuple, TypeConversion<T> wrapper) {
                String key = project.process(tuple, -1).get(0);
		keys.add(wrapper.fromString(key));
	}

	private static <JAT extends Comparable<JAT>> double computeLowerProb(
			FrequencyPosition fp, int sp, JAT k, JoinMatrix<JAT> joinMatrix,
			boolean isX, NumericConversion wrapper) {
		if (k.equals(wrapper.getMinValue()) || k.equals(wrapper.getMaxValue())) {
			// the extreme values are always entirely included
			return 0;
		}
		if (fp.getFrequency() == 0) {
			// this may happen for x2 and y2, the probability is 0
			return 0;
		}
		// if we are here, sp is a meaningful number
		// obtain a probability that a tuple with such a key goes to this
		// KeyRegion (Joiner)
		double startPos = fp.getPosition() - sp;
		// endPos is non-inclusive !!!!!
		double total_k = 0;
		if (isX) {
			total_k = joinMatrix.getNumXElements(k);
		} else {
			total_k = joinMatrix.getNumYElements(k);
		}
		return startPos / total_k;
	}

	private static <JAT extends Comparable<JAT>> double computeUpperProb(
			FrequencyPosition fp, int sp, JAT k, JoinMatrix<JAT> joinMatrix,
			boolean isX, NumericConversion wrapper) {
		if (k.equals(wrapper.getMinValue()) || k.equals(wrapper.getMaxValue())) {
			// the extreme values are always entirely included
			return 1;
		}
		if (fp.getFrequency() == 0) {
			// this may happen for x2 and y2, the probability is 0
			return 0;
		}
		// if we are here, sp is a meningfull number
		// obtain a probability that a tuple with such a key goes to this
		// KeyRegion (Joiner)
		double startPos = fp.getPosition() - sp;
		// endPos is non-inclusive !!!!!
		double endPos = startPos + fp.getFrequency();
		double total_k = 0;
		if (isX) {
			total_k = joinMatrix.getNumXElements(k);
		} else {
			total_k = joinMatrix.getNumYElements(k);
		}
		return endPos / total_k;
	}

	private static <T extends Comparable<T>> void fillMatrix(
			JoinMatrix joinMatrix, List<T> firstKeys, List<T> secondKeys,
			ComparisonPredicate comparison) {
		LOG.info("Started fillMatrix method at " + getWallClockTime());
		int xSize = firstKeys.size();
		int ySize = secondKeys.size();

		// enter join attributes
		for (int i = 0; i < xSize; i++) {
			joinMatrix.setJoinAttributeX(firstKeys.get(i));
		}
		for (int i = 0; i < ySize; i++) {
			joinMatrix.setJoinAttributeY(secondKeys.get(i));
		}

		LOG.info("Set tuple keys, just about to fill the matrix at "
				+ getWallClockTime());

		fillOutput(joinMatrix, firstKeys, secondKeys, comparison);

		LOG.info("Ended fillMatrix method at " + getWallClockTime());
	}

	private static <T extends Comparable<T>> void fillOutput(
			JoinMatrix joinMatrix, List<T> firstKeys, List<T> secondKeys,
			ComparisonPredicate comparison) {

		int xSize = firstKeys.size();
		int ySize = secondKeys.size();
		int numXBuckets = 50, numYBuckets = 50;
		if (xSize < numXBuckets) {
			numXBuckets = xSize;
		}
		if (ySize < numYBuckets) {
			numYBuckets = ySize;
		}
		StringBuilder sb = new StringBuilder();

		InputShallowCoarsener ic = new InputShallowCoarsener(numXBuckets,
				numYBuckets);
		ic.setOriginalMatrix(joinMatrix, sb);

		for (int i = 0; i < numXBuckets; i++) {
			int x1 = ic.getOriginalXCoordinate(i, false);
			int x2 = ic.getOriginalXCoordinate(i, true);
			for (int j = 0; j < numYBuckets; j++) {
				int y1 = ic.getOriginalYCoordinate(j, false);
				int y2 = ic.getOriginalYCoordinate(j, true);
				Region region = new Region(x1, y1, x2, y2);
				// monotonic optimization not worthed - this class is obsolete
				if (MyUtilities.isCandidateRegion(joinMatrix, region,
						comparison, _map)) {
					// only candidates should be analyzed for setting to 1
					for (int x = x1; x <= x2; x++) {
						for (int y = y1; y <= y2; y++) {
							if (comparison.test(firstKeys.get(x),
									secondKeys.get(y))) {
								joinMatrix.setElement(1, x, y);
							}
						}
					}
				}
			}
		}
	}

	public static <JAT extends Comparable<JAT>> List<KeyRegion> generateKeyRegions(
			List<Region> regions, JoinMatrix<JAT> joinMatrix,
			NumericConversion wrapper) {
		List<KeyRegion> keyRegions = new ArrayList<KeyRegion>();
		joinMatrix.precomputeFrequencies();
		int index = 0;
		for (Region region : regions) {
			int x1 = region.get_x1();
			int y1 = region.get_y1();
			// the upper positions of x and y are not inclusive - necessary for
			// not missing a cell
			int x2 = region.get_x2() + 1;
			int y2 = region.get_y2() + 1;

			// obtain keys from positions
			JAT kx1, ky1, kx2, ky2;
			// get first key positions
			int spkx1, spky1, spkx2, spky2;

			if (x1 == 0) {
				kx1 = (JAT) wrapper.getMinValue();
				spkx1 = 0;
			} else {
				kx1 = joinMatrix.getJoinAttributeX(x1);
				spkx1 = joinMatrix.getXFirstKeyPosition(kx1);
			}
			if (y1 == 0) {
				ky1 = (JAT) wrapper.getMinValue();
				spky1 = 0;
			} else {
				ky1 = joinMatrix.getJoinAttributeY(y1);
				spky1 = joinMatrix.getYFirstKeyPosition(ky1);
			}
			if (x2 == joinMatrix.getXSize()) {
				kx2 = (JAT) wrapper.getMaxValue();
				spkx2 = 0;
			} else {
				kx2 = joinMatrix.getJoinAttributeX(x2);
				spkx2 = joinMatrix.getXFirstKeyPosition(kx2);
			}
			if (y2 == joinMatrix.getYSize()) {
				ky2 = (JAT) wrapper.getMaxValue();
				spky2 = 0;
			} else {
				ky2 = joinMatrix.getJoinAttributeY(y2);
				spky2 = joinMatrix.getYFirstKeyPosition(ky2);
			}

			// FrequencyPosition:
			// frequency: how many tuples with a given key inside the region
			// position: the lowest position of the key in the region (the
			// position absolute to the joinMatrix)
			FrequencyPosition fpx1 = joinMatrix.getXFreqPos(kx1, region);
			FrequencyPosition fpy1 = joinMatrix.getYFreqPos(ky1, region);
			FrequencyPosition fpx2 = joinMatrix.getXFreqPos(kx2, region);
			FrequencyPosition fpy2 = joinMatrix.getYFreqPos(ky2, region);

			// ------------------------------
			double prob_kx1Lower = computeLowerProb(fpx1, spkx1, kx1,
					joinMatrix, true, wrapper);
			double prob_kx1Upper = computeUpperProb(fpx1, spkx1, kx1,
					joinMatrix, true, wrapper);

			double prob_ky1Lower = computeLowerProb(fpy1, spky1, ky1,
					joinMatrix, false, wrapper);
			double prob_ky1Upper = computeUpperProb(fpy1, spky1, ky1,
					joinMatrix, false, wrapper);

			double prob_kx2Lower = computeLowerProb(fpx2, spkx2, kx2,
					joinMatrix, true, wrapper);
			double prob_kx2Upper = computeUpperProb(fpx2, spkx2, kx2,
					joinMatrix, true, wrapper);

			double prob_ky2Lower = computeLowerProb(fpy2, spky2, ky2,
					joinMatrix, false, wrapper);
			double prob_ky2Upper = computeUpperProb(fpy2, spky2, ky2,
					joinMatrix, false, wrapper);
			// ------------------------------

			// finally, creating and adding the region
			KeyRegion keyRegion = new KeyRegion(kx1, ky1, kx2, ky2,
					prob_kx1Lower, prob_kx1Upper, prob_ky1Lower, prob_ky1Upper,
					prob_kx2Lower, prob_kx2Upper, prob_ky2Lower, prob_ky2Upper,
					index);
			index++;
			keyRegions.add(keyRegion);
		}
		return keyRegions;
	}

	public static String getWallClockTime() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		return dateFormat.format(cal.getTime());
	}

	private static Logger LOG = Logger.getLogger(PushStatisticCollector.class);

	private static java.util.logging.Logger _fileLogger;

	private long SLEEP_MILLIS_END = 10000;

	/*
	 * private static <T extends Comparable<T>> void fillOutputOld(JoinMatrix
	 * joinMatrix, List<T> firstKeys, List<T> secondKeys, ComparisonPredicate
	 * comparison){
	 * 
	 * int xSize = firstKeys.size(); int ySize = secondKeys.size();
	 * 
	 * // enter actual matrix (output tuples) for(int i = 0; i < xSize; i++){
	 * for(int j = 0 ; j < ySize; j++){ if(comparison.test(firstKeys.get(i),
	 * secondKeys.get(j))){ joinMatrix.setElement(1, i, j); } } } }
	 */

	private PLCQueryPlan _queryPlan = null;

	private static Map _map;

	private List<TilingAlgorithm> _algorithms;

	private int _numLastJoiners; // extracted from _map in chooseQueryPlan

	public PushStatisticCollector(Map map) {
		_map = map;
		chooseQueryPlan();
		specifyAlgorithms();
		createFileLogger();
	}

	private void chooseQueryPlan() {
		String queryName = SystemParameters.getString(_map, "DIP_QUERY_NAME");
		_queryPlan = null;
		if (queryName.equalsIgnoreCase("hyracks")
				|| queryName.equalsIgnoreCase("theta_hyracks")) {
			_queryPlan = new Hyracks(_map);
			_numLastJoiners = SystemParameters.getInt(_map,
					"JOINERS_CUSTOMER_ORDERS_PAR");
		} else if (queryName.equalsIgnoreCase("theta_lines_self_join")) {
			_queryPlan = new ThetaLineitemSelfJoin(_map);
			_numLastJoiners = SystemParameters.getInt(_map,
					"JOINERS_LINEITEM1_LINEITEM2_PAR");
		} else if (queryName
				.equalsIgnoreCase("theta_lines_self_join_input_dominated")) {
			_queryPlan = new ThetaLineitemSelfJoinInputDominated(_map);
			_numLastJoiners = SystemParameters.getInt(_map,
					"JOINERS_LINEITEM1_LINEITEM2_PAR");
		} else if (queryName.equalsIgnoreCase("theta_tpch5_R_N_S_L")) {
			_queryPlan = new ThetaTPCH5_R_N_S_L(_map);
			_numLastJoiners = SystemParameters.getInt(_map,
					"JOINERS_REGION_NATION_SUPPLIER_LINEITEM_PAR");
		} else if (queryName.equalsIgnoreCase("theta_tpch7_L_S_N1")) {
			_queryPlan = new ThetaTPCH7_L_S_N1(_map);
			_numLastJoiners = SystemParameters.getInt(_map,
					"JOINERS_LINEITEM_SUPPLIER_NATION1_PAR");
		} else {
			throw new RuntimeException("Unsupported query plan " + queryName
					+ "!");
		}
	}

	private void createFileLogger() {
		_fileLogger = java.util.logging.Logger.getLogger("MyLog");
		FileHandler fh;

		try {
			// This block configure the logger with handler and formatter
			String path = SystemParameters.getString(_map,
					"DIP_OPTIMALITY_LOGS") + "/optimality.info";
			fh = new FileHandler(path, true); // append = true
			_fileLogger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void finalizeProcessing() {
		try {
			boolean toVisualize = false;
			boolean toWriteMatrix = false;
			// boolean toWriteKeyRegions = SystemParameters.getBoolean(_map,
			// "DIP_WRITE_KEY_REGIONS");
			boolean toWriteKeyRegions = false;
			LOG.info("Starting with creating matrix at " + getWallClockTime());

			// generate matrix
			JoinMatrix joinMatrix = _queryPlan.generateMatrix();
			NumericConversion wrapper = _queryPlan.getWrapper();
			String queryName = SystemParameters.getString(_map,
					"DIP_QUERY_NAME");
			String dataPath = SystemParameters.getString(_map, "DIP_DATA_PATH");

			if (toWriteMatrix) {
				// optionally write matrix to a file
				joinMatrix.writeMatrixToFile();
			}

			if (toVisualize) {
				// visualize without regions
				String label = queryName;
				VisualizerInterface visualizer = new UJMPVisualizer(label);
				joinMatrix.visualize(visualizer);
			}
			LOG.info("Done with creating matrix at " + getWallClockTime());

			long startTime = 0;
			for (TilingAlgorithm algorithm : _algorithms) {
				try {

					// BSP algorithms modify join matrix
					// to run multiple BSP in a row, we do a deep copy
					long regenerateMatrixStart = System.currentTimeMillis();
					// joinMatrix = joinMatrix.getDeepCopy(); it does not work
					// correctly
					joinMatrix = _queryPlan.generateMatrix();
					double regenerateMatrixTime = (System.currentTimeMillis() - regenerateMatrixStart) / 1000.0;
					LOG.info("\nRegenerate matrix time is "
							+ regenerateMatrixTime + "seconds.\n");

					// print info
					StringBuilder sb = new StringBuilder();
					sb.append("\n").append(queryName).append(" in ")
							.append(dataPath);
					sb.append(" of size [").append(joinMatrix.getXSize())
							.append(", ").append(joinMatrix.getYSize())
							.append("]\n");
					sb.append("The number of joiners is ")
							.append(_numLastJoiners).append("\n");
					sb.append(algorithm.toString()).append("\n");
					_fileLogger.info(sb.toString());

					// run algorithm
					startTime = System.currentTimeMillis();
					sb = new StringBuilder();
					List<Region> regions = algorithm.partition(joinMatrix, sb);
					long endTime = System.currentTimeMillis();
					double elapsed = (endTime - startTime) / 1000.0;

					// compute the optimality
					OptimalityMetricInterface opt = null;
					WeightPrecomputation wp = algorithm.getPrecomputation();
					if (wp != null) {
						opt = new MaxAvgOptimality(joinMatrix, regions, wp);
					} else {
						opt = new MaxAvgOptimality(joinMatrix, regions,
								algorithm.getWeightFunction());
					}

					// compute the joiner regions
					List<KeyRegion> keyRegions = generateKeyRegions(regions,
							joinMatrix, wrapper);
					String queryId = MyUtilities.getQueryID(_map);
					String filename = SystemParameters.getString(_map,
							"DIP_KEY_REGION_ROOT") + "/" + queryId;
					if (toWriteKeyRegions) {
						DeepCopy.serializeToFile(keyRegions, filename);
						keyRegions = (List<KeyRegion>) DeepCopy
								.deserializeFromFile(filename);
					}
					LOG.info(KeyRegion.toString(keyRegions));

					// print regions and optimality
					sb.append("Final regions are: ").append(
							Region.toString(regions, opt, "Final"));
					sb.append("\nElapsed algorithm time is ").append(elapsed)
							.append(" seconds.\n");
					sb.append("\n[").append(opt.toString()).append("] is ")
							.append(opt.getOptimalityDistance()).append("\n");
					sb.append("\n=========================================================================================\n");
					_fileLogger.info(sb.toString());

					if (toVisualize) {
						// visualize with regions
						JoinMatrix matrixCopy = joinMatrix.getDeepCopy(); // in
						// order
						// not
						// to
						// spoil
						// the
						// original
						matrixCopy.setRegions(regions);
						String label = queryName + " with regions";
						VisualizerInterface visualizer = new UJMPVisualizer(
								label);
						matrixCopy.visualize(visualizer);

						// press Enter to finish
						LOG.info("Press Enter to close GUIs ...");
						System.in.read();
					}

				} catch (Exception exc) {
					LOG.info("EXCEPTION" + MyUtilities.getStackTrace(exc));
				}
			}

		} catch (Exception exc) {
			LOG.info("EXCEPTION" + MyUtilities.getStackTrace(exc));
		}
	}

	public void processTuple(List<String> tuple, int relationNumber) {
		_queryPlan.processTuple(tuple, relationNumber);
	}

	// change from here down
	private void specifyAlgorithms() {
		// read constants if they exist
		int okcanP = 100;
		if (SystemParameters.isExisting(_map, "DIP_OKCAN_P")) {
			okcanP = SystemParameters.getInt(_map, "DIP_OKCAN_P");
		}
		int bspP = 50;
		if (SystemParameters.isExisting(_map, "DIP_BSP_P")) {
			bspP = SystemParameters.getInt(_map, "DIP_BSP_P");
		}

		// -------
		// !!!! WATCH OUT: IF YOU USE TWO BSP ALGORITHMS, CHECK WHAT DO YOU DO
		// WITH ORIGINAL JOIN_MATRIX
		_algorithms = new ArrayList<TilingAlgorithm>();
		WeightFunction wf = new WeightFunction(1, 1);
		// _algorithms.add(new OkcanCandidateInputAlgorithm(_numLastJoiners, wf,
		// okcanP, okcanP, _map));
		// _algorithms.add(new OkcanCandidateOutputAlgorithm(_numLastJoiners,
		// wf, okcanP, okcanP, _map));
		_algorithms.add(new OkcanExactInputAlgorithm(_numLastJoiners, wf,
				okcanP, okcanP, _map));
		_algorithms.add(new OkcanExactOutputAlgorithm(_numLastJoiners, wf,
				okcanP, okcanP, _map));
		ShallowCoarsener inputCoarsener = new InputShallowCoarsener(bspP, bspP);
		// _algorithms.add(new BSPAlgorithm(_map, _numLastJoiners, wf,
		// inputCoarsener, BSPAlgorithm.COVERAGE_MODE.DENSE));
		_algorithms.add(new BSPAlgorithm(_map, _numLastJoiners, wf,
				inputCoarsener, BSPAlgorithm.COVERAGE_MODE.SPARSE));
		// ShallowCoarsener outputCoarsener = new OutputShallowCoarsener(bspP,
		// bspP, wf, _map);
		// _algorithms.add(new BSPAlgorithm(_map, _numLastJoiners, wf,
		// outputCoarsener, BSPAlgorithm.MODE.DENSE));
		// _algorithms.add(new BSPAlgorithm(_map, _numLastJoiners, wf,
		// outputCoarsener, BSPAlgorithm.MODE.SPARSE));
		ShallowCoarsener inputOutputCoarsener = new InputOutputShallowCoarsener(
				bspP, bspP, wf, _map);
		_algorithms.add(new BSPAlgorithm(_map, _numLastJoiners, wf,
				inputOutputCoarsener));
	}
}
