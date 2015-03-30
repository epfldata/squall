package ch.epfl.data.squall.ewh.utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import ch.epfl.data.squall.conversion.NumericConversion;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.utilities.DeepCopy;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.SystemParameters.HistogramType;

// This class is used only for connecting other components to D2Component

// Divides into contiguous key ranges, and assign keys around the boundary to multiple targets
//    ranges are [beginning, end)
// To be parameterized with KeyType, we need to put some of the logic into the invoking StormComponent class
//    in that case, we can replace getDistance method with compareTo
public class RangeMulticastStreamGrouping implements CustomStreamGrouping,
		Serializable {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger
			.getLogger(RangeMulticastStreamGrouping.class);

	// the number of tasks on the level this stream grouping is sending to
	private int _numTargetTasks;
	private List<Integer> _targetTasks;
	private List _rangeBoundaries = new ArrayList(); // (numTargetTasks - 1) of
														// them

	protected final Map _map;
	protected NumericConversion _wrapper;
	private ComparisonPredicate _comparison;
	private int _numLastJoiners;
	private HistogramType _histType;

	// with multicast
	public RangeMulticastStreamGrouping(Map map,
			ComparisonPredicate comparison, NumericConversion wrapper,
			HistogramType histType) {
		this(map, wrapper, histType);
		_comparison = comparison;
	}

	// without multicast
	public RangeMulticastStreamGrouping(Map map, NumericConversion wrapper,
			HistogramType histType) {
		_map = map;
		_wrapper = wrapper;

		// this constructor is always invoked, directly or indirectly
		_histType = histType;
		_numLastJoiners = SystemParameters.getInt(_map, "PAR_LAST_JOINERS");
		if (histType != null) {
			// this is in the constructor, as it requires reading files from
			// local machine
			_rangeBoundaries = createBoundariesFromHistogram(
					histType.filePrefix(), _numLastJoiners);
		}
	}

	private void checkBoundaries() {
		// TODO We require that no neighbor elements in _rangeBoundaries are the
		// same
		// Otherwise, RangeMulticastStreamGrouping.chooseTargetIndexNonMulticast
		// does not work
		// We will implement this only if we encounter the following runtime
		// exception
		// To do it, we need to add some probabilities, similar to
		// ContentSensitiveMatrixAssignment
		Object lastBoundary = null;
		for (Object boundary : _rangeBoundaries) {
			if (boundary.equals(lastBoundary)) {
				// it suffices to compare only neighbors, as _rangeBoundaries
				// are sorted, by definition
				throw new RuntimeException(
						"Two neighbor boundaries are the same: " + boundary);
			}
			lastBoundary = boundary;
		}

		// another checkup
		if (_rangeBoundaries.size() != _numTargetTasks - 1) {
			throw new RuntimeException("Developer error: Boundaries size is "
					+ _rangeBoundaries.size() + " and _numTargetTasks = "
					+ _numTargetTasks);
		}
	}

	private List<Integer> chooseTargetIndex(String tupleHash) {
		List<Integer> targetIndexes = new ArrayList<Integer>();
		Object hash = _wrapper.fromString(tupleHash);

		if (_numTargetTasks <= 1) {
			// 1 target tasks, everything goes there
			targetIndexes.add(0);
		} else {
			if (isMulticast()) {
				targetIndexes = chooseTargetIndexMulticast(hash);
			} else {
				targetIndexes = Arrays
						.asList(chooseTargetIndexNonMulticast(hash));
			}
		}
		return targetIndexes;
	}

	// works for _numTargetTasks >= 2
	private List<Integer> chooseTargetIndexMulticast(Object hash) {
		int op = _comparison.getOperation();
		if (op == ComparisonPredicate.EQUAL_OP) {
			return Arrays.asList(chooseTargetIndexNonMulticast(hash));
		} else if (op == ComparisonPredicate.SYM_BAND_WITH_BOUNDS_OP
				|| op == ComparisonPredicate.SYM_BAND_NO_BOUNDS_OP) {
			// diff is inclusive
			int diff = (Integer) _comparison.getDiff();
			if (op == ComparisonPredicate.SYM_BAND_NO_BOUNDS_OP) {
				diff--;
			}

			// it returns all the indexes in range of
			// [chooseTargetIndexNonMulticast(hash - diff),
			// chooseTargetIndexNonMulticast(hash + diff)]
			Object minJoinable = _wrapper.getOffset(hash, -diff);
			int minJoinableTargetIndex = chooseTargetIndexNonMulticast(minJoinable);
			Object maxJoinable = _wrapper.getOffset(hash, diff);
			int maxJoinableTargetIndex = chooseTargetIndexNonMulticast(maxJoinable);

			List<Integer> targetIndexes = new ArrayList<Integer>();
			for (int i = minJoinableTargetIndex; i <= maxJoinableTargetIndex; i++) {
				targetIndexes.add(i);
			}
			return targetIndexes;
		} else {
			throw new RuntimeException("Unsupported operator " + op);
		}
	}

	private int chooseTargetIndexNonMulticast(Object hash) {
		return chooseTaskIndex(hash, _rangeBoundaries);
	}

	// numTargetTasks = rangeBoundaries.size() + 1
	protected int chooseTaskIndex(Object key, List rangeBoundaries) {
		List<Integer> taskIndexes = new ArrayList<Integer>();
		int numTasks = rangeBoundaries.size() + 1;
		if (rangeBoundaries.isEmpty()) {
			// redundant for this class but not for its child
			taskIndexes.add(0);
			return taskIndexes.get(0);
		}

		// check for first and last
		// d2Min and d2Max are different from firstBoundary and lastBoundary
		Object firstBoundary = rangeBoundaries.get(0);
		Object lastBoundary = rangeBoundaries.get(numTasks - 2);
		if (_wrapper.getDistance(firstBoundary, key) > 0) {
			taskIndexes.add(0);
		} else if (_wrapper.getDistance(key, lastBoundary) >= 0) {
			taskIndexes.add(numTasks - 1);
		}

		// check for others
		for (int i = 0; i < numTasks - 2; i++) {
			Object lowerBound = rangeBoundaries.get(i);
			Object upperBound = rangeBoundaries.get(i + 1);
			if (_wrapper.getDistance(key, lowerBound) >= 0
					&& _wrapper.getDistance(upperBound, key) > 0) {
				taskIndexes.add(i + 1);
			}
		}

		if (taskIndexes.size() != 1) {
			throw new RuntimeException(
					"Developer error! Should not have size(targets) = "
							+ taskIndexes.size());
		}

		return taskIndexes.get(0);
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> stormTuple) {
		final List<String> tuple = (List<String>) stormTuple.get(1);
		final String tupleHash = (String) stormTuple.get(2);
		if (MyUtilities.isFinalAck(tuple, _map))
			// send to everyone
			return _targetTasks;
		else {
			List<Integer> targetIndexes = chooseTargetIndex(tupleHash);
			List<Integer> tupleTargetTasks = new ArrayList<Integer>();
			for (int targetIndex : targetIndexes) {
				tupleTargetTasks.add(_targetTasks.get(targetIndex));
			}
			return tupleTargetTasks;
		}
	}

	// there are parallelism(ex _numTargetTasks) - 1 boundaries
	protected List createBoundariesFromHistogram(String filePrefix,
			int parallelism) {
		List result = new ArrayList();
		// no boundaries if only parallelism = 1
		if (parallelism > 1) {
			String histogramFilename = MyUtilities.getHistogramFilename(_map,
					parallelism, filePrefix);
			LOG.info("HistogramFilename for " + filePrefix + " = "
					+ histogramFilename);

			// "Most impressive is that the entire process is JVM independent,
			// meaning an object can be serialized on one platform and
			// deserialized on an entirely different platform."
			result = (List) DeepCopy.deserializeFromFile(histogramFilename);
			LOG.info("Boundaries are " + result);
		}
		return result;
	}

	// divides MIN and MAX values from conf into _numTargetTasks segments
	// there are _numTargetTasks - 1 boundaries
	private void createBoundariesStatic() {
		// no boundaries if only one targetTask
		if (_numTargetTasks > 1) {
			String d2MinStr = SystemParameters.getString(_map, "D2_EQUI_MIN");
			String d2MaxStr = SystemParameters.getString(_map, "D2_EQUI_MAX");
			Object d2Min = _wrapper.fromString(d2MinStr);
			Object d2Max = _wrapper.fromString(d2MaxStr);
			int distance = (int) _wrapper.getDistance(d2Max, d2Min);
			int unitDistance = distance / _numTargetTasks;
			for (int i = 0; i < _numTargetTasks - 1; i++) {
				Object boundary = _wrapper.getOffset(d2Min, (i + 1)
						* unitDistance);
				_rangeBoundaries.add(boundary);
				LOG.info("Added boundary " + boundary);
			}
		}
	}

	private boolean isMulticast() {
		return _comparison != null;
	}

	@Override
	public void prepare(WorkerTopologyContext wtc, GlobalStreamId gsi,
			List<Integer> targetTasks) {
		// LOG.info("Target tasks for Range(Multicast) are " + targetTasks);
		// seems sorted on each producer
		_targetTasks = targetTasks;
		_numTargetTasks = targetTasks.size();
		// This class is used only for connecting other components to
		// D2Component
		if (_histType == null) {
			// _numTargetTasks can differ from PAR_LAST_JOINERS - that's why we
			// do this in the prepare method
			createBoundariesStatic();
		} else {
			if (_numTargetTasks != _numLastJoiners) {
				throw new RuntimeException(
						"_numTargetTasks = "
								+ _numTargetTasks
								+ ", _numLastJoiners = "
								+ _numLastJoiners
								+ " and they differ.\n"
								+ " One should not need it, but if needs, deserialization in createBoundariesFromHistogram should be changed.");
			}
		}
		// check in both cases
		checkBoundaries();
	}
}