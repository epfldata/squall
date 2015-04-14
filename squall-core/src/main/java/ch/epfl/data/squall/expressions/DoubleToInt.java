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

import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.visitors.ValueExpressionVisitor;

//translates double 123.45 to 12345
public class DoubleToInt implements ValueExpression<Integer> {
    private static final long serialVersionUID = 1L;

    private final Type<Integer> _wrapper = new IntegerType();

    private int _columnIndex;

    public DoubleToInt(int columnIndex) {
	_columnIndex = columnIndex;
    }

    @Override
    public void accept(ValueExpressionVisitor vev) {
	throw new RuntimeException("Not implemented for a moment!");
	// vev.visit(this);
    }

    // unused
    @Override
    public void changeValues(int i, ValueExpression<Integer> newExpr) {
	// nothing
    }

    @Override
    public Integer eval(List<String> tuple) {
	String value = tuple.get(_columnIndex);
	value = value.replace(".", "");
	return _wrapper.fromString(value);
    }

    @Override
    public String evalString(List<String> tuple) {
	final int result = eval(tuple);
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
	sb.append("DoubleToInt ").append(_columnIndex);
	return sb.toString();
    }
}