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

package ch.epfl.data.squall.operators;

import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class SampleOperator extends OneToOneOperator implements Operator {
    private static Logger LOG = Logger.getLogger(SampleOperator.class);
    private static final long serialVersionUID = 1L;

    private double _sampleRate = 0;
    private int _numTuplesProcessed = 0;
    private Random _rnd = new Random();

    public SampleOperator(int relationSize, int numOfBuckets) {
	_sampleRate = ((double) (numOfBuckets * SystemParameters.TUPLES_PER_BUCKET))
		/ relationSize;
	if (_sampleRate >= 1) {
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
	throw new RuntimeException(
		"getContent for SampleOperator should never be invoked!");
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
		"printContent for SampleOperator should never be invoked!");
    }

    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
	_numTuplesProcessed++;
	if (_rnd.nextDouble() < _sampleRate) {
	    return tuple;
	} else {
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
