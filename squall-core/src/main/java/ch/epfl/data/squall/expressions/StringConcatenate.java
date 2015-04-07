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

import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.visitors.ValueExpressionVisitor;

public class StringConcatenate implements ValueExpression<String> {
	private static final long serialVersionUID = 1L;

	private final List<ValueExpression<String>> _strList = new ArrayList<ValueExpression<String>>();

	public StringConcatenate(ValueExpression<String> str1,
			ValueExpression<String> str2, ValueExpression<String>... strArray) {
		_strList.add(str1);
		_strList.add(str2);
		_strList.addAll(Arrays.asList(strArray));
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		vev.visit(this);
	}

	@Override
	public void changeValues(int i, ValueExpression<String> newExpr) {

	}

	@Override
	public String eval(List<String> tuple) {
		String result = "";
		for (final ValueExpression<String> str : _strList)
			result += str;
		return result;
	}

	@Override
	public String evalString(List<String> tuple) {
		return eval(tuple);
	}

	@Override
	public List<ValueExpression> getInnerExpressions() {
		return MyUtilities.listTypeErasure(_strList);
	}

	@Override
	public Type getType() {
		return new StringType();
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
		for (int i = 0; i < _strList.size(); i++) {
			sb.append("(").append(_strList.get(i)).append(")");
			if (i != _strList.size() - 1)
				sb.append(" STR_CONCAT ");
		}
		return sb.toString();
	}

}