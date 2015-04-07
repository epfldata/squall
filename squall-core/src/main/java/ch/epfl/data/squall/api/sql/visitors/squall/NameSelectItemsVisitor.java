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


package ch.epfl.data.squall.api.sql.visitors.squall;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import ch.epfl.data.squall.api.sql.optimizers.name.NameTranslator;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.util.TupleSchema;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;

public class NameSelectItemsVisitor extends IndexSelectItemsVisitor {
	private final NameTranslator _nt;

	private final TupleSchema _tupleSchema;

	private final static StringType _sc = new StringType();

	public NameSelectItemsVisitor(TupleSchema tupleSchema, Map map,
			Component affectedComponent) {
		super(map);

		_tupleSchema = tupleSchema;
		_nt = new NameTranslator(affectedComponent.getName());
	}

	/*
	 * Has to be separate because ExpressionList does not extend Expression
	 */
	private boolean isRecognized(ExpressionList params) {
		if (params == null)
			return true;

		final List<Expression> exprs = params.getExpressions();
		if (exprs == null || exprs.isEmpty())
			return true;

		// if any of the children is not recognized, we have to go to super
		// if some of exprs are recognized and some not, we will have some extra
		// elements on stack
		for (final Expression expr : exprs)
			if (!isRecognized(expr))
				return false;

		// all exprs recognized
		return true;
	}

	/*
	 * returns true if an expression was found in tupleSchema true means no need
	 * to call parent, somthing added to stack It has side effects - putting on
	 * exprStack
	 */
	private <T extends Expression> boolean isRecognized(T expr) {
		// expr is changed in place, so that it does not contain synonims
		final int position = _nt.indexOf(_tupleSchema, expr);
		if (position != ParserUtil.NOT_FOUND) {
			// we found an expression already in the tuple schema
			final Type tc = _nt.getType(_tupleSchema, expr);
			final ValueExpression ve = new ColumnReference(tc, position,
					ParserUtil.getStringExpr(expr));
			pushToExprStack(ve);
			return true;
		} else
			return false;
	}

	@Override
	public void visit(Addition adtn) {
		if (!isRecognized(adtn))
			// normal call to parent
			super.visit(adtn);
	}

	/*
	 * only getColumnIndex method invocation is different than in parent
	 */
	@Override
	public void visit(Column column) {
		// extract the position (index) of the required column
		// column might be changed, due to the synonym effect
		final int position = _nt.getColumnIndex(_tupleSchema, column);

		// extract type for the column
		// TypeConversion tc = _nt.getType(_tupleSchema, column);

		// TODO: Due to the fact that Project prepares columns for FinalAgg on
		// the last component
		// and that for SUM or COUNT this method is not invoked (recognize is
		// true),
		// but only for GroupByProjections as the top level method.
		// That is, we can safely assume StringConversion method.
		// Permanent fix is to create StringConversion over overallAggregation.
		final Type tc = _sc;

		final ValueExpression ve = new ColumnReference(tc, position,
				ParserUtil.getStringExpr(column));
		pushToExprStack(ve);
	}

	@Override
	public void visit(Division dvsn) {
		if (!isRecognized(dvsn))
			// normal call to parent
			super.visit(dvsn);
	}

	@Override
	public void visit(Function function) {
		boolean recognized = isRecognized(function);
		if (!recognized) {
			// try to extract SUM
			// parameters for COUNT are ignored, as explained in super
			final String fnName = function.getName();
			if (fnName.equalsIgnoreCase("SUM")) {
				recognized = isRecognized(function.getParameters());
				if (recognized) {
					final ValueExpression expr = popFromExprStack();
					createSum(expr, function.isDistinct());
				}
			} else if (fnName.equalsIgnoreCase("COUNT")) {
				final List<ValueExpression> expressions = new ArrayList<ValueExpression>();

				if (function.isDistinct()) {
					// putting something on stack only if isDistinct is set to
					// true
					recognized = isRecognized(function.getParameters());
					// it might happen that we put on stack something we don't
					// use
					// this is the case only when some exprs are recognized and
					// the others not
					if (recognized) {

						// create a list of expressions
						int numParams = 0;
						final ExpressionList params = function.getParameters();
						if (params != null)
							numParams = params.getExpressions().size();

						for (int i = 0; i < numParams; i++)
							expressions.add(popFromExprStack());
					}
				} else
					recognized = true;
				if (recognized)
					// finally, create CountAgg out of expressions (empty if
					// nonDistinct)
					createCount(expressions, function.isDistinct());
			}
		}
		if (!recognized)
			// normal call to parent
			super.visit(function);
	}

	@Override
	public void visit(Multiplication m) {
		if (!isRecognized(m))
			// normal call to parent
			super.visit(m);
	}

	@Override
	public void visit(Parenthesis prnths) {
		if (!isRecognized(prnths))
			// normal call to parent
			super.visit(prnths);
	}

	@Override
	public void visit(Subtraction s) {
		if (!isRecognized(s))
			// normal call to parent
			super.visit(s);
	}

}
