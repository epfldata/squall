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

import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class SelectOperator implements Operator {
    private static final long serialVersionUID = 1L;

    private final Predicate _predicate;

    private int _numTuplesProcessed = 0;

    public SelectOperator(Predicate predicate) {
	_predicate = predicate;
    }

    @Override
    public void accept(OperatorVisitor ov) {
	ov.visit(this);
    }

    @Override
    public List<String> getContent() {
	throw new RuntimeException(
		"getContent for SelectionOperator should never be invoked!");
    }

    @Override
    public int getNumTuplesProcessed() {
	return _numTuplesProcessed;
    }

    public Predicate getPredicate() {
	return _predicate;
    }

    @Override
    public boolean isBlocking() {
	return false;
    }

    @Override
    public String printContent() {
	throw new RuntimeException(
		"printContent for SelectionOperator should never be invoked!");
    }

    @Override
    public List<String> process(List<String> tuple, long lineageTimestamp) {
	_numTuplesProcessed++;
	if (_predicate.test(tuple))
	    return tuple;
	else
	    return null;
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("SelectOperator with Predicate: ");
	sb.append(_predicate.toString());
	return sb.toString();
    }
}
