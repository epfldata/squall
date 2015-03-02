package plan_runner.operators;

import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import plan_runner.predicates.Predicate;
import plan_runner.utilities.SystemParameters;
import plan_runner.visitors.OperatorVisitor;

public class SampleOperator implements Operator {
	private static Logger LOG = Logger.getLogger(SampleOperator.class);
	private static final long serialVersionUID = 1L;
	
	private double _sampleRate = 0;
	private int _numTuplesProcessed = 0;
	private Random _rnd = new Random();

	public SampleOperator(int relationSize, int numOfBuckets) {
		_sampleRate = ((double) (numOfBuckets * SystemParameters.TUPLES_PER_BUCKET)) / relationSize;
		if(_sampleRate >= 1){
			_sampleRate = 1;
		}
		LOG.info("Sample rate is " + _sampleRate);
	}

	@Override
	public void accept(OperatorVisitor ov) {
		ov.visit(this);
	}

	@Override
	public List<String> getContent() {
		throw new RuntimeException("getContent for SampleOperator should never be invoked!");
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
		throw new RuntimeException("printContent for SampleOperator should never be invoked!");
	}

	@Override
	public List<String> process(List<String> tuple) {
		_numTuplesProcessed++;
		if(_rnd.nextDouble() < _sampleRate){
			return tuple;
		}else{
			return null;
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("SampleOperator with Sample Rate: ");
		sb.append(_sampleRate);
		return sb.toString();
	}
}