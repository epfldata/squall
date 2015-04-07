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

import java.util.List;

import ch.epfl.data.squall.visitors.PredicateVisitor;

public class booleanPrimitive implements Predicate {

	private boolean _bool;

	public booleanPrimitive(boolean bool) {
		_bool = bool;
	}

	@Override
	public void accept(PredicateVisitor pv) {
		pv.visit(this);

	}

	@Override
	public List<Predicate> getInnerPredicates() {
		return null;
	}

	@Override
	public boolean test(List<String> tupleValues) {
		return _bool;
	}

	@Override
	public boolean test(List<String> firstTupleValues,
			List<String> secondTupleValues) {
		return _bool;
	}

}
