/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ch.epfl.data.squall.ewh.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class SampleAsideAndForwardOperator implements Operator {
    private static Logger LOG = Logger
	    .getLogger(SampleAsideAndForwardOperator.class);
    private static final long serialVersionUID = 1L;

    private double _sampleRate = 0;
    private int _numTuplesProcessed = 0;
    private Random _rnd = new Random();

    private String _componentIndex;
    private List<Integer> _hashIndexes = new ArrayList<Integer>(
	    Arrays.asList(0)); // we receive one-column tuples
    private Map _conf;

    // it's not clear design to put _collector in here, but we opted for it in
    // order to allow the operator to be anywhere in the chain
    private SpoutOutputCollector _spoutCollector;
    private OutputCollector _boltCollector;
    private String _streamId;

    public SampleAsideAndForwardOperator(int relationSize, int numOfBuckets,
	    String streamId, Map conf) {
	_conf = conf;

	_streamId = streamId;

	_sampleRate = ((double) (numOfBuckets * SystemParameters.TUPLES_PER_BUCKET))
		/ relationSize;
	if (_sampleRate >= 1) {
	    _sampleRate = 1;
	}
	LOG.info("Sample rate of SampleAsideAndForwardOperator is "
		+ _sampleRate);
    }

    // invoked from open methods of StormBoltComponent (not known beforehand)
    public void setCollector(OutputCollector collector) {
	_boltCollector = collector;
    }

    public void setCollector(SpoutOutputCollector collector) {
	_spoutCollector = collector;
    }

    public void setComponentIndex(String hostComponentIndex) {
	_componentIndex = hostComponentIndex;
    }

    private boolean isAttachedToSpout() {
	return _spoutCollector != null;
    }

    @Override
    public void accept(OperatorVisitor ov) {
	ov.visit(this);
    }

    @Override
    public List<String> getContent() {
	throw new RuntimeException(
		"getContent for SampleAsideAndForwardOperator should never be invoked!");
    }

    @Override
    public int getNumTuplesProcessed() {
	return _numTuplesProcessed;
    }

    public double getSampleRate() {
	return _sampleRate;
    }

    @Override
    public boolean isBlocking() {
	return false;
    }

    @Override
    public String printContent() {
	throw new RuntimeException(
		"printContent for SampleAsideAndForwardOperator should never be invoked!");
    }

    @Override
    public List<String> process(List<String> tuple, long lineageTimestamp) {
	_numTuplesProcessed++;

	// sending to this extra streamId
	if (_rnd.nextDouble() < _sampleRate) {
	    Values stormTupleSnd = MyUtilities.createTupleValues(tuple, 0,
		    _componentIndex, _hashIndexes, null, _conf);
	    if (isAttachedToSpout()) {
		_spoutCollector.emit(_streamId, stormTupleSnd);
	    } else {
		_boltCollector.emit(_streamId, stormTupleSnd);
	    }
	}

	// normal forwarding
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