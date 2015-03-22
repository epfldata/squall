package ch.epfl.data.plan_runner.storm_components;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.SystemParameters;

public class StormSrcHarmonizer extends BaseRichBolt implements StormComponent {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(StormSrcHarmonizer.class);

	private OutputCollector _collector;
	private final Map _conf;

	private final String _ID;
	private final String _componentName;
	private final StormEmitter _firstEmitter, _secondEmitter;

	private int _numRemainingParents;

	public StormSrcHarmonizer(String componentName, StormEmitter firstEmitter,
			StormEmitter secondEmitter, TopologyBuilder builder,
			TopologyKiller killer, Config conf) {
		_conf = conf;
		_componentName = componentName;

		_firstEmitter = firstEmitter;
		_secondEmitter = secondEmitter;

		_ID = componentName + "_HARM";

		final int parallelism = SystemParameters.getInt(conf, _componentName
				+ "_PAR");
		InputDeclarer currentBolt = builder.setBolt(_ID, this, parallelism);
		currentBolt = MyUtilities.attachEmitterHash(_conf, null, currentBolt,
				_firstEmitter, _secondEmitter);
	}

	@Override
	public void aggBatchSend() {
		throw new RuntimeException("Should not be here!");
	}

	// from IRichBolt
	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("CompIndex", "Tuple", "Hash"));
	}

	@Override
	public void execute(Tuple stormRcvTuple) {
		final String inputComponentIndex = stormRcvTuple.getString(0);
		final List<String> tuple = (List<String>) stormRcvTuple.getValue(1);
		final String inputTupleHash = stormRcvTuple.getString(2);

		if (MyUtilities.isFinalAck(tuple, _conf)) {
			_numRemainingParents--;
			MyUtilities.processFinalAck(_numRemainingParents,
					StormComponent.INTERMEDIATE, _conf, stormRcvTuple,
					_collector);
			return;
		}

		_collector.emit(stormRcvTuple, new Values(inputComponentIndex, tuple,
				inputTupleHash));
		_collector.ack(stormRcvTuple);
	}

	// from StormComponent
	@Override
	public String getID() {
		return _ID;
	}

	@Override
	public String getInfoID() {
		final String str = "Harmonizer " + _componentName + " has ID: " + _ID;
		return str;
	}

	@Override
	public void prepare(Map conf, TopologyContext tc, OutputCollector collector) {
		_collector = collector;
		_numRemainingParents = MyUtilities.getNumParentTasks(tc, _firstEmitter,
				_secondEmitter);
	}

	@Override
	public void printContent() {
		// this class has no content: this is purposely empty
	}

	@Override
	public void printTuple(List<String> tuple) {
		// this is purposely empty
	}

	@Override
	public void printTupleLatency(long numSentTuples, long timestamp) {
		// empty
	}

	@Override
	public void tupleSend(List<String> tuple, Tuple stormTupleRcv,
			long timestamp) {
		throw new RuntimeException("Should not be here!");
	}

}