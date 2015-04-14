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

package ch.epfl.data.squall.predicates;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.epfl.data.squall.visitors.PredicateVisitor;

public class OrPredicate implements Predicate {
    private static final long serialVersionUID = 1L;

    private final List<Predicate> _predicateList = new ArrayList<Predicate>();

    public OrPredicate(Predicate pred1, Predicate pred2,
	    Predicate... predicateArray) {
	_predicateList.add(pred1);
	_predicateList.add(pred2);
	_predicateList.addAll(Arrays.asList(predicateArray));
    }

    @Override
    public void accept(PredicateVisitor pv) {
	pv.visit(this);
    }

    @Override
    public List<Predicate> getInnerPredicates() {
	return _predicateList;
    }

    @Override
    public boolean test(List<String> tupleValues) {
	for (final Predicate pred : _predicateList)
	    if (pred.test(tupleValues) == true)
		return true;
	return false;
    }

    @Override
    public boolean test(List<String> firstTupleValues,
	    List<String> secondTupleValues) {
	for (final Predicate pred : _predicateList)
	    if (pred.test(firstTupleValues, secondTupleValues) == true)
		return true;
	return false;
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	for (int i = 0; i < _predicateList.size(); i++) {
	    sb.append("(").append(_predicateList.get(i)).append(")");
	    if (i != _predicateList.size() - 1)
		sb.append(" OR ");
	}
	return sb.toString();
    }

}