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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.visitors.ValueExpressionVisitor;

/*
 * This class implements Addition between any Number type (Integer, Double, Long, etc.).
 * It convert all the value to double, and then return the final result by automatic casting
 *   (i.e. (int)1.0 )
 *
 * Double can store integers exactly in binary representation,
 *   so we won't lose the precision on our integer operations.
 *
 * Having different T types in the constructor arguments
 *   does not result in exception in the constructor,
 *   but rather in eval method.
 */
public class Addition<T extends Number & Comparable<T>> implements
		ValueExpression<T> {

	private static final long serialVersionUID = 1L;

	private final List<ValueExpression> _veList = new ArrayList<ValueExpression>();
	private final NumericType<T> _wrapper;

	public Addition(ValueExpression ve1, ValueExpression ve2,
			ValueExpression... veArray) {
		_veList.add(ve1);
		_veList.add(ve2);
		_veList.addAll(Arrays.asList(veArray));
		_wrapper = (NumericType<T>) MyUtilities
				.getDominantNumericType(_veList);
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		vev.visit(this);
	}

	@Override
	public void changeValues(int i, ValueExpression<T> newExpr) {
		_veList.remove(i);
		_veList.add(i, newExpr);
	}

	@Override
	public T eval(List<String> tuple) {
		double result = 0;
		for (final ValueExpression factor : _veList) {
			final Object currentVal = factor.eval(tuple);
			final NumericType currentType = (NumericType) (factor
					.getType());
			result += currentType.toDouble(currentVal);
		}
		return _wrapper.fromDouble(result);
	}

	@Override
	public String evalString(List<String> tuple) {
		final T result = eval(tuple);
		return _wrapper.toString(result);
	}

	@Override
	public List<ValueExpression> getInnerExpressions() {
		return _veList;
	}

	@Override
	public Type getType() {
		return _wrapper;
	}

	@Override
	public void inverseNumber() {

	}

	@Override
	public boolean isNegative() {
		return false;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < _veList.size(); i++) {
			sb.append("(").append(_veList.get(i)).append(")");
			if (i != _veList.size() - 1)
				sb.append(" + ");
		}
		return sb.toString();
	}

}