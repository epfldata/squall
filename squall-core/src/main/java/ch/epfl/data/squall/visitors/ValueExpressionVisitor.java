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


package ch.epfl.data.squall.visitors;

import ch.epfl.data.squall.expressions.Addition;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.DateDiff;
import ch.epfl.data.squall.expressions.DateSum;
import ch.epfl.data.squall.expressions.Division;
import ch.epfl.data.squall.expressions.IntegerYearFromDate;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.StringConcatenate;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueSpecification;

public interface ValueExpressionVisitor {
	public void visit(Addition add);

	public void visit(ColumnReference cr);

	public void visit(DateDiff dd);

	public void visit(DateSum ds);

	public void visit(Division dvsn);

	public void visit(IntegerYearFromDate iyfd);

	public void visit(Multiplication mult);

	public void visit(StringConcatenate sc);

	public void visit(Subtraction sub);

	public void visit(ValueSpecification vs);
}
