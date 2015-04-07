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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import ch.epfl.data.squall.api.sql.optimizers.name.NameTranslator;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.util.TupleSchema;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.IntegerYearFromDate;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.types.DateType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;

/*
 * Used for Cost-optimizer(nameTranslator)
 *   to convert a list of JSQL expressions to a list of Squall ValueExpressions
 * Similar to IndexWhereVisitor, but does not use predStack
 */
public class NameProjectVisitor implements ExpressionVisitor, ItemsListVisitor {
	private final NameTranslator _nt;

	private final TupleSchema _inputTupleSchema;
	private final Stack<ValueExpression> _exprStack = new Stack<ValueExpression>();

	// this will not break any contracts,
	// even with new DateConversion() on all the places,
	// we will have a single object per (possibly) multiple spout/bolt threads.
	// generating plans is done from a single thread, static additionally saves
	// space
	private static LongType _lc = new LongType();
	private static DoubleType _dblConv = new DoubleType();
	private static DateType _dateConv = new DateType();
	private static StringType _sc = new StringType();

	public NameProjectVisitor(TupleSchema inputTupleSchema,
			Component affectedComponent) {
		_inputTupleSchema = inputTupleSchema;

		_nt = new NameTranslator(affectedComponent.getName());
	}

	public List<ValueExpression> getExprs() {
		final List<ValueExpression> veList = new ArrayList<ValueExpression>();
		while (!_exprStack.isEmpty())
			veList.add(_exprStack.pop());
		Collections.reverse(veList); // stack inverse the order of elements
		return veList;
	}

	/*
	 * returns true if an expression was found in tupleSchema true means no need
	 * to call parent It has side effects - putting on exprStack
	 */
	private <T extends Expression> boolean isRecognized(T expr) {
		// expr is changed in place, so that it does not contain synonims
		final int position = _nt.indexOf(_inputTupleSchema, expr);
		if (position != ParserUtil.NOT_FOUND) {
			// we found an expression already in the tuple schema
			final Type tc = _nt.getType(_inputTupleSchema, expr);
			final ValueExpression ve = new ColumnReference(tc, position,
					ParserUtil.getStringExpr(expr));
			pushToExprStack(ve);
			return true;
		} else
			return false;
	}

	private void pushToExprStack(ValueExpression ve) {
		_exprStack.push(ve);
	}

	@Override
	public void visit(Addition adtn) {
		if (!isRecognized(adtn)) {
			// normal call
			visitBinaryOperation(adtn);

			final ValueExpression right = _exprStack.pop();
			final ValueExpression left = _exprStack.pop();

			final ValueExpression add = new ch.epfl.data.squall.expressions.Addition(
					left, right);
			_exprStack.push(add);
		}
	}

	@Override
	public void visit(AllComparisonExpression ace) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(AndExpression ae) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(AnyComparisonExpression ace) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Between btwn) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(BitwiseAnd ba) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(BitwiseOr bo) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(BitwiseXor bx) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(CaseExpression ce) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Column column) {
		// extract the position (index) of the required column
		// column might be changed, due to the synonim effect
		final int position = _nt.getColumnIndex(_inputTupleSchema, column);

		// extract type for the column
		final Type tc = _nt.getType(_inputTupleSchema, column);

		final ValueExpression ve = new ColumnReference(tc, position,
				ParserUtil.getStringExpr(column));
		pushToExprStack(ve);
	}

	@Override
	public void visit(Concat concat) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(DateValue dv) {
		final ValueExpression ve = new ValueSpecification(_dateConv,
				dv.getValue());
		_exprStack.push(ve);
	}

	@Override
	public void visit(Division dvsn) {
		if (!isRecognized(dvsn)) {
			// normal call
			visitBinaryOperation(dvsn);

			final ValueExpression right = _exprStack.pop();
			final ValueExpression left = _exprStack.pop();

			final ValueExpression add = new ch.epfl.data.squall.expressions.Division(
					left, right);
			_exprStack.push(add);
		}
	}

	@Override
	public void visit(DoubleValue dv) {
		final ValueExpression ve = new ValueSpecification(_dblConv,
				dv.getValue());
		_exprStack.push(ve);
	}

	@Override
	public void visit(EqualsTo et) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(ExistsExpression ee) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(ExpressionList el) {
		for (final Iterator iter = el.getExpressions().iterator(); iter
				.hasNext();) {
			final Expression expression = (Expression) iter.next();
			expression.accept(this);
		}
	}

	/*
	 * We have the similar code in Index Visitors. It has to be there in case
	 * that we decide not to do projections on complex expression as soon as
	 * possible (for example when we need to send both EXTRACT_YEAR(ORDERDATE)
	 * and ORDERDATE itself)
	 */
	@Override
	public void visit(Function function) {
		if (!isRecognized(function)) {
			// normal call
			// all aggregate functions (SUM, AVG, COUNT, MAX, MIN) should never
			// appear here
			final ExpressionList params = function.getParameters();
			int numParams = 0;
			if (params != null) {
				params.accept(this);

				// in order to determine the size
				final List<Expression> listParams = params.getExpressions();
				numParams = listParams.size();
			}
			final List<ValueExpression> expressions = new ArrayList<ValueExpression>();
			for (int i = 0; i < numParams; i++)
				expressions.add(_exprStack.pop());
			Collections.reverse(expressions); // at the stack top is the lastly
			// added VE

			final String fnName = function.getName();
			if (fnName.equalsIgnoreCase("EXTRACT_YEAR")) {
				if (numParams != 1)
					throw new RuntimeException(
							"EXTRACT_YEAR function has exactly one parameter!");
				final ValueExpression expr = expressions.get(0);
				final ValueExpression ve = new IntegerYearFromDate(expr);
				_exprStack.push(ve);
			}
		}
	}

	@Override
	public void visit(GreaterThan gt) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(GreaterThanEquals gte) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(InExpression ie) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(InverseExpression ie) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(IsNullExpression ine) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(JdbcParameter jp) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(LikeExpression le) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void visit(List<Expression> exprs) {
		for (final Expression expr : exprs)
			expr.accept(this);
	}

	@Override
	public void visit(LongValue lv) {
		final ValueExpression ve = new ValueSpecification(_lc, lv.getValue());
		_exprStack.push(ve);
	}

	@Override
	public void visit(Matches mtchs) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(MinorThan mt) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(MinorThanEquals mte) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Multiplication m) {
		if (!isRecognized(m)) {
			// normal call
			visitBinaryOperation(m);

			final ValueExpression right = _exprStack.pop();
			final ValueExpression left = _exprStack.pop();

			final ValueExpression add = new ch.epfl.data.squall.expressions.Multiplication(
					left, right);
			_exprStack.push(add);
		}
	}

	@Override
	public void visit(NotEqualsTo net) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	// not used
	@Override
	public void visit(NullValue nv) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(OrExpression oe) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Parenthesis prnths) {
		// ValueExpression tree contains information about precedence
		// this is why ValueExpression there is no ParenthesisValueExpression
		if (!isRecognized(prnths))
			prnths.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue sv) {
		final ValueExpression ve = new ValueSpecification(_sc, sv.getValue());
		_exprStack.push(ve);
	}

	@Override
	public void visit(SubSelect ss) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Subtraction s) {
		if (!isRecognized(s)) {
			// normal call
			visitBinaryOperation(s);

			final ValueExpression right = _exprStack.pop();
			final ValueExpression left = _exprStack.pop();

			final ValueExpression add = new ch.epfl.data.squall.expressions.Subtraction(
					left, right);
			_exprStack.push(add);
		}
	}

	@Override
	public void visit(TimestampValue tv) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(TimeValue tv) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(WhenClause wc) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	private void visitBinaryOperation(BinaryExpression be) {
		be.getLeftExpression().accept(this);
		be.getRightExpression().accept(this);
	}

}