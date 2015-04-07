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


package ch.epfl.data.squall.expressions;

import java.util.List;

import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.visitors.ValueExpressionVisitor;

//translates phone in a form with dashes (e.g 88-9) into a Long
public class LongPhone implements ValueExpression<Long> {
	private static final long serialVersionUID = 1L;

	private final Type<Long> _wrapper = new LongType();

	private int _columnIndex;
	private int _firstDigits = -1;

	public LongPhone(int columnIndex) {
		_columnIndex = columnIndex;
	}

	public LongPhone(int columnIndex, int firstDigits) {
		this(columnIndex);
		_firstDigits = firstDigits;
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		throw new RuntimeException("Not implemented for a moment!");
		// vev.visit(this);
	}

	// unused
	@Override
	public void changeValues(int i, ValueExpression<Long> newExpr) {
		// nothing
	}

	@Override
	public Long eval(List<String> tuple) {
		String value = tuple.get(_columnIndex);
		value = value.replace("-", "");
		if (_firstDigits != -1) {
			value = value.substring(0, _firstDigits);
		}
		return _wrapper.fromString(value);
	}

	@Override
	public String evalString(List<String> tuple) {
		final long result = eval(tuple);
		return _wrapper.toString(result);
	}

	@Override
	public List<ValueExpression> getInnerExpressions() {
		return null;
	}

	@Override
	public Type getType() {
		return _wrapper;
	}

	@Override
	public void inverseNumber() {
		// nothing

	}

	@Override
	public boolean isNegative() {
		return false;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("LongPhone ").append(_columnIndex);
		return sb.toString();
	}
}