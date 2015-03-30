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
import java.util.List;

import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.visitors.PredicateVisitor;

/*
 * ve1 LIKE ve2 (bigger smaller)
 * WORKS ONLY for pattern '%value%'
 */
public class LikePredicate implements Predicate {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final ValueExpression<String> _ve1;
	private ValueExpression<String> _ve2;

	public LikePredicate(ValueExpression<String> ve1,
			ValueExpression<String> ve2) {
		_ve1 = ve1;
		_ve2 = ve2;
		// WORKS ONLY for pattern '%value%'
		if (_ve2 instanceof ValueSpecification) {
			String value = _ve2.eval(null);
			value = value.replace("%", "");
			_ve2 = new ValueSpecification<String>(new StringConversion(), value);
		}
	}

	@Override
	public void accept(PredicateVisitor pv) {
		pv.visit(this);
	}

	public List<ValueExpression> getExpressions() {
		final List<ValueExpression> result = new ArrayList<ValueExpression>();
		result.add(_ve1);
		result.add(_ve2);
		return result;
	}

	@Override
	public List<Predicate> getInnerPredicates() {
		return new ArrayList<Predicate>();
	}

	@Override
	public boolean test(List<String> tupleValues) {
		final String val1 = _ve1.eval(tupleValues);
		final String val2 = _ve2.eval(tupleValues);
		return val1.contains(val2);
	}

	@Override
	public boolean test(List<String> firstTupleValues,
			List<String> secondTupleValues) {
		final String val1 = _ve1.eval(firstTupleValues);
		final String val2 = _ve2.eval(firstTupleValues);
		return val1.contains(val2);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(_ve1.toString());
		sb.append(" LIKE ");
		sb.append(_ve2.toString());
		return sb.toString();
	}

}