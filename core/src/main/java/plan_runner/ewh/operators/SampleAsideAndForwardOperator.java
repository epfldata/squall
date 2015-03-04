package plan_runner.ewh.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

import plan_runner.operators.Operator;
import plan_runner.predicates.Predicate;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import plan_runner.visitors.OperatorVisitor;

public class SampleAsideAndForwardOperator implements Operator {
	private static Logger LOG = Logger.getLogger(SampleAsideAndForwardOperator.class);
	private static final long serialVersionUID = 1L;
	
	private double _sampleRate = 0;
	private int _numTuplesProcessed = 0;
	private Random _rnd = new Random();

	private String _componentIndex;
	private List<Integer> _hashIndexes = new ArrayList<Integer>(Arrays.asList(0)); // we receive one-column tuples
	private Map _conf;
	
	// it's not clear design to put _collector in here, but we opted for it in order to allow the operator to be anywhere in the chain
	private SpoutOutputCollector _spoutCollector;
	private OutputCollector _boltCollector;
	private String _streamId;
	
	public SampleAsideAndForwardOperator(int relationSize, int numOfBuckets, String streamId, Map conf) {
		_conf = conf;
		
		_streamId = streamId;
		
		_sampleRate = ((double) (numOfBuckets * SystemParameters.TUPLES_PER_BUCKET)) / relationSize;
		if(_sampleRate >= 1){
			_sampleRate = 1;
		}
		LOG.info("Sample rate of SampleAsideAndForwardOperator is " + _sampleRate);
	}
	
	// invoked from open methods of StormBoltComponent (not known beforehand)
	public void setCollector(OutputCollector collector) {
		_boltCollector = collector;
	}
	
	public void setCollector(SpoutOutputCollector collector) {
		_spoutCollector = collector;
	}
	
	public void setComponentIndex(String hostComponentIndex){
		_componentIndex = hostComponentIndex;
	}
	
	private boolean isAttachedToSpout(){
		return _spoutCollector != null;
	}

	@Override
	public void accept(OperatorVisitor ov) {
		ov.visit(this);
	}

	@Override
	public List<String> getContent() {
		throw new RuntimeException("getContent for SampleAsideAndForwardOperator should never be invoked!");
	}

	@Override
	public int getNumTuplesProcessed() {
		return _numTuplesProcessed;
	}
	
	public double getSampleRate(){
		return _sampleRate;
	}

	@Override
	public boolean isBlocking() {
		return false;
	}

	@Override
	public String printContent() {
		throw new RuntimeException("printContent for SampleAsideAndForwardOperator should never be invoked!");
	}

	@Override
	public List<String> process(List<String> tuple) {
		_numTuplesProcessed++;
		
		//sending to this extra streamId
		if(_rnd.nextDouble() < _sampleRate){
			Values stormTupleSnd = MyUtilities.createTupleValues(tuple, 0, _componentIndex, _hashIndexes, null, _conf);
			if(isAttachedToSpout()){
				_spoutCollector.emit(_streamId, stormTupleSnd);
			}else{
				_boltCollector.emit(_streamId, stormTupleSnd);
			}
		}		
		
		//normal forwarding
		return tuple;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("SampleAsideAndForwardOperator with Sample Rate: ");
		sb.append(_sampleRate);
		return sb.toString();
	}
}