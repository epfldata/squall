package ch.epfl.data.plan_runner.ewh.storm_components;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.StormEmitter;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.DeepCopy;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.SystemParameters.HistogramType;

//equi-depth histogram on one or both input relations
public class CreateHistogramBolt<JAT extends Number & Comparable<JAT>> extends
	BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(CreateHistogramBolt.class);

    private StormEmitter _r1Emitter, _r2Emitter;
    private String _r1EmitterIndex, _r2EmitterIndex;
    private final String _componentName;
    private NumericConversion _wrapper;
    private ComparisonPredicate _comparison;
    private Map _conf;
    private OutputCollector _collector;

    private int _numRemainingParents;
    private int _numOfLastJoiners; // input to tiling algorithm

    // private ListAdapter<JAT> _joinKeys = new ListJavaGeneric<JAT>();
    // private ListAdapter<JAT> _joinKeys = new ListTIntAdapter();
    // to avoid some boxing/unboxing, we could directly use TIntList

    private List<JAT> _joinKeys1 = new ArrayList<JAT>();
    private List<JAT> _joinKeys2 = new ArrayList<JAT>();

    public CreateHistogramBolt(StormEmitter r1Source, StormEmitter r2Source,
	    String componentName, int numOfLastJoiners,
	    NumericConversion<JAT> wrapper, ComparisonPredicate comparison,
	    List<String> allCompNames, TopologyBuilder builder,
	    TopologyKiller killer, Config conf) {

	if (r2Source != null) { // is the data source which feeds D2Combiner
	    _r2Emitter = r2Source;
	    _r2EmitterIndex = String.valueOf(allCompNames.indexOf(r2Source
		    .getName()));
	}
	if (r1Source != null) { // is the data source one which feeds
				// S1Reservoir directly
	    _r1Emitter = r1Source;
	    _r1EmitterIndex = String.valueOf(allCompNames.indexOf(r1Source
		    .getName()));
	}
	_componentName = componentName;

	_numOfLastJoiners = numOfLastJoiners;
	_conf = conf;
	_comparison = comparison;
	_wrapper = wrapper;

	// _joinKeys = MyUtilities.createListAdapter(conf);

	final int parallelism = 1;

	// connecting with previous level
	InputDeclarer currentBolt = builder.setBolt(componentName, this,
		parallelism);

	if (_r1Emitter != null) {
	    currentBolt = MyUtilities.attachEmitterToSingle(currentBolt,
		    _r1Emitter);
	}
	if (_r2Emitter != null) {
	    currentBolt = MyUtilities.attachEmitterToSingle(currentBolt,
		    _r2Emitter);
	}

	// connecting with Killer
	// if (getHierarchyPosition() == FINAL_COMPONENT &&
	// (!MyUtilities.isAckEveryTuple(conf)))
	killer.registerComponent(this, componentName, parallelism);
    }

    @Override
    public void execute(Tuple stormTupleRcv) {
	final String inputComponentIndex = stormTupleRcv
		.getStringByField(StormComponent.COMP_INDEX); // getString(0);
	final List<String> tuple = (List<String>) stormTupleRcv
		.getValueByField(StormComponent.TUPLE); // getValue(1);

	if (processFinalAck(tuple, stormTupleRcv))
	    return;

	processNonLastTuple(inputComponentIndex, tuple, stormTupleRcv, true);

	_collector.ack(stormTupleRcv);
    }

    // from IRichBolt
    @Override
    public Map<String, Object> getComponentConfiguration() {
	return _conf;
    }

    private void processNonLastTuple(String inputComponentIndex,
	    List<String> tuple, Tuple stormTupleRcv, boolean isLastInBatch) {

	if (inputComponentIndex.equals(_r1EmitterIndex)) {
	    String key = tuple.get(0); // key is the only thing sent
	    _joinKeys1.add((JAT) _wrapper.fromString(key));
	} else if (inputComponentIndex.equals(_r2EmitterIndex)) {
	    String key = tuple.get(0); // key is the only thing sent
	    _joinKeys2.add((JAT) _wrapper.fromString(key));
	} else {
	    throw new RuntimeException("Unrecognized data source!");
	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// if (_hierarchyPosition == FINAL_COMPONENT) { // then its an
	// intermediate
	// stage not the final
	// one
	// if (!MyUtilities.isAckEveryTuple(_conf))
	declarer.declareStream(SystemParameters.EOF_STREAM, new Fields(
		SystemParameters.EOF));
    }

    // BaseRichSpout
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
	// create list of non-null emitters
	List<StormEmitter> emitters = new ArrayList<StormEmitter>();
	if (_r1Emitter != null) {
	    emitters.add(_r1Emitter);
	}
	if (_r2Emitter != null) {
	    emitters.add(_r2Emitter);
	}

	_collector = collector;
	_numRemainingParents = MyUtilities.getNumParentTasks(tc, emitters);
    }

    // if true, we should exit from method which called this method
    protected boolean processFinalAck(List<String> tuple, Tuple stormTupleRcv) {
	if (MyUtilities.isFinalAck(tuple, _conf)) {
	    _numRemainingParents--;
	    if (_numRemainingParents == 0) {
		finalizeProcessing();
	    }
	    MyUtilities.processFinalAck(_numRemainingParents,
		    StormComponent.FINAL_COMPONENT, _conf, stormTupleRcv,
		    _collector);
	    return true;
	}
	return false;
    }

    private void finalizeProcessing() {
	if (_r1Emitter != null) {
	    createHistogram(_joinKeys1, HistogramType.S1_RES_HIST.filePrefix());
	}
	if (_r2Emitter != null) {
	    createHistogram(_joinKeys2, HistogramType.D2_COMB_HIST.filePrefix());
	}
    }

    private void createHistogram(List<JAT> joinKeys, String filePrefix) {
	LOG.info("Before sorting keys");
	// joinKeys.sort();
	Collections.sort(joinKeys);
	LOG.info("After sorting keys");
	LOG.info("Keys size is " + joinKeys.size());

	// create bucket boundaries
	// choose keys equi-distantly such that in total there are
	// _numOfLastJoiners of them
	List<JAT> boundaries = createBoundaries(joinKeys, _numOfLastJoiners);
	int size = boundaries.size();
	LOG.info("Boundaries size is " + size);
	if (size != _numOfLastJoiners - 1) {
	    throw new RuntimeException(
		    "Developer error! RangeMulticastStreamGrouping expects _numOfLastJoiners - 1 bondaries!");
	}

	String histogramFilename = MyUtilities.getHistogramFilename(_conf,
		_numOfLastJoiners, filePrefix);
	LOG.info("HistogramFilename " + filePrefix + " = " + histogramFilename);

	// write Histogram
	// "Most impressive is that the entire process is JVM independent,
	// meaning an object can be serialized on one platform and deserialized
	// on an entirely different platform."
	DeepCopy.serializeToFile(boundaries, histogramFilename);
	boundaries = (List) DeepCopy.deserializeFromFile(histogramFilename);
	LOG.info("Boundaries are " + boundaries);
    }

    private List<JAT> createBoundaries(List<JAT> joinKeys, int numOfBuckets) {
	if (numOfBuckets > joinKeys.size()) {
	    throw new RuntimeException(
		    "numOfJoiners at the last component has to be bigger than the number of join keys");
	}
	double distance = ((double) joinKeys.size()) / numOfBuckets;
	if (distance < 1) {
	    throw new RuntimeException(
		    "A bug: same element cannot be included more than once!");
	}

	// ListAdapter<JAT> boundaries = MyUtilities.createListAdapter(_conf);
	List<JAT> boundaries = new ArrayList<JAT>();
	for (int i = 0; i < numOfBuckets; i++) {
	    // We want to avoid high discrepancy between bucket sizes, which is
	    // a consequence of input sample size != 100 * n_s
	    int index = (int) (i * distance + 0.5);
	    boundaries.add(joinKeys.get(index));
	}

	// RangeMulticastStreamGrouping does not need the start of the first
	// bucket (it is -infinity anyway)
	boundaries.remove(0);

	return boundaries;
    }
}