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

import java.io.Serializable;
import java.util.List;

import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.visitors.ValueExpressionVisitor;

public interface ValueExpression<T extends Comparable<T>> extends Serializable {
	public void accept(ValueExpressionVisitor vev);

	// matt...
	public void changeValues(int i, ValueExpression<T> newExpr);

	public T eval(List<String> tuple);

	public String evalString(List<String> tuple);

	// not ValueExpression<T> because inside might be other type(as in
	// IntegerYearFromDate)
	public List<ValueExpression> getInnerExpressions();

	public TypeConversion getType();

	public void inverseNumber();

	public boolean isNegative();
}