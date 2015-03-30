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
import java.util.Date;
import java.util.List;

import ch.epfl.data.squall.conversion.DateConversion;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.visitors.ValueExpressionVisitor;

public class DateDiff implements ValueExpression<Integer> {
	private static final long serialVersionUID = 1L;

	private final TypeConversion<Date> _dc = new DateConversion();
	private final TypeConversion<Integer> _ic = new IntegerConversion();

	private final ValueExpression<Date> _ve1, _ve2;

	public DateDiff(ValueExpression<Date> ve1, ValueExpression<Date> ve2) {
		_ve1 = ve1;
		_ve2 = ve2;
	}

	@Override
	public void accept(ValueExpressionVisitor vev) {
		vev.visit(this);
	}

	@Override
	public void changeValues(int i, ValueExpression<Integer> newExpr) {
		// nothing
	}

	@Override
	public Integer eval(List<String> tuple) {
		Date dateObj1 = _ve1.eval(tuple);
		Date dateObj2 = _ve2.eval(tuple);

		long diff = dateObj2.getTime() - dateObj1.getTime();
		int diffDays = (int) (diff / (24 * 1000 * 60 * 60));

		return diffDays;
	}

	@Override
	public String evalString(List<String> tuple) {
		return _ic.toString(eval(tuple));
	}

	@Override
	public List<ValueExpression> getInnerExpressions() {
		final List<ValueExpression> result = new ArrayList<ValueExpression>();
		result.add(_ve1);
		result.add(_ve2);
		return result;
	}

	@Override
	public TypeConversion getType() {
		return _ic;
	}

	@Override
	public void inverseNumber() {
		// nothing
	}

	@Override
	public boolean isNegative() {
		// nothing
		return false;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("DateDiff ");
		sb.append("First Exp: ").append(_ve1.toString()).append("\n");
		sb.append("Second Exp: ").append(_ve2.toString()).append("\n");
		return sb.toString();
	}
}