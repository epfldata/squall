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
import ch.epfl.data.squall.api.sql.optimizers.index.IndexTranslator;
import ch.epfl.data.squall.api.sql.schema.Schema;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.util.TableAliasName;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.IntegerYearFromDate;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.AndPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.LikePredicate;
import ch.epfl.data.squall.predicates.OrPredicate;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.types.DateType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;

/*
 * Translates JSQL expressions to a SelectionOperator of a component.
 *   JSQL expressions *must* refer only to the component.
 */
public class IndexWhereVisitor implements ExpressionVisitor, ItemsListVisitor {
	private final Stack<ValueExpression> _exprStack = new Stack<ValueExpression>();
	private final Stack<Predicate> _predStack = new Stack<Predicate>();

	// all of these are necessary only for getColumnIndex in visitColumn method
	private Component _affectedComponent;
	private Schema _schema;
	private TableAliasName _tan;
	private IndexTranslator _it;

	// this will not break any contracts,
	// even with new DateConversion() on all the places,
	// we will have a single object per (possibly) multiple spout/bolt threads.
	// generating plans is done from a single thread, static additionally saves
	// space
	private static LongType _lc = new LongType();
	private static DoubleType _dblConv = new DoubleType();
	private static DateType _dateConv = new DateType();
	private static StringType _sc = new StringType();

	/*
	 * Used from NameWhereVisitor - no parameters need to be set
	 */
	protected IndexWhereVisitor() {
	}

	public IndexWhereVisitor(Component affectedComponent, Schema schema,
			TableAliasName tan) {
		_affectedComponent = affectedComponent;
		_schema = schema;
		_tan = tan;
		_it = new IndexTranslator(_schema, _tan);

		_affectedComponent = affectedComponent;
	}

	public SelectOperator getSelectOperator() {
		if (_predStack.size() != 1)
			throw new RuntimeException(
					"After WhereVisitor is done, it should contain one predicate exactly!");
		return new SelectOperator(_predStack.peek());
	}

	protected void pushToExprStack(ValueExpression ve) {
		_exprStack.push(ve);
	}

	/*
	 * Each of these operations create a Squall type, that's why so much similar
	 * code
	 */
	@Override
	public void visit(Addition adtn) {
		visitBinaryOperation(adtn);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ValueExpression add = new ch.epfl.data.squall.expressions.Addition(
				left, right);
		_exprStack.push(add);
	}

	@Override
	public void visit(AllComparisonExpression ace) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(AndExpression ae) {
		visitBinaryOperation(ae);

		final Predicate right = _predStack.pop();
		final Predicate left = _predStack.pop();

		final Predicate and = new AndPredicate(left, right);
		_predStack.push(and);
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
		// extract type for the column
		final Type tc = _schema.getType(ParserUtil
				.getFullSchemaColumnName(column, _tan));

		// extract the position (index) of the required column
		final int position = _it.getColumnIndex(column, _affectedComponent);

		final ValueExpression ve = new ColumnReference(tc, position);
		_exprStack.push(ve);
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
		visitBinaryOperation(dvsn);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ValueExpression division = new ch.epfl.data.squall.expressions.Division(
				left, right);
		_exprStack.push(division);
	}

	@Override
	public void visit(DoubleValue dv) {
		final ValueExpression ve = new ValueSpecification(_dblConv,
				dv.getValue());
		_exprStack.push(ve);
	}

	@Override
	public void visit(EqualsTo et) {
		visitBinaryOperation(et);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ComparisonPredicate cp = new ComparisonPredicate(
				ComparisonPredicate.EQUAL_OP, left, right);
		_predStack.push(cp);
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

	@Override
	public void visit(Function function) {
		// all aggregate functions (SUM, AVG, COUNT, MAX, MIN) have only one
		// parameter (Expression)
		// although COUNT(*) has no parameters
		// EXTRACT_YEAR has one parameter
		// if you change this method, NameProjectVisitor.visit(Function) has to
		// be changed as well
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

	@Override
	public void visit(GreaterThan gt) {
		visitBinaryOperation(gt);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ComparisonPredicate cp = new ComparisonPredicate(
				ComparisonPredicate.GREATER_OP, left, right);
		_predStack.push(cp);
	}

	@Override
	public void visit(GreaterThanEquals gte) {
		visitBinaryOperation(gte);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ComparisonPredicate cp = new ComparisonPredicate(
				ComparisonPredicate.NONLESS_OP, left, right);
		_predStack.push(cp);
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
		visitBinaryOperation(le);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final LikePredicate lp = new LikePredicate(left, right);
		_predStack.push(lp);
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
		visitBinaryOperation(mt);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ComparisonPredicate cp = new ComparisonPredicate(
				ComparisonPredicate.LESS_OP, left, right);
		_predStack.push(cp);
	}

	@Override
	public void visit(MinorThanEquals mte) {
		visitBinaryOperation(mte);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ComparisonPredicate cp = new ComparisonPredicate(
				ComparisonPredicate.NONGREATER_OP, left, right);
		_predStack.push(cp);
	}

	@Override
	public void visit(Multiplication m) {
		visitBinaryOperation(m);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ValueExpression mult = new ch.epfl.data.squall.expressions.Multiplication(
				left, right);
		_exprStack.push(mult);
	}

	@Override
	public void visit(NotEqualsTo net) {
		visitBinaryOperation(net);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ComparisonPredicate cp = new ComparisonPredicate(
				ComparisonPredicate.NONEQUAL_OP, left, right);
		_predStack.push(cp);
	}

	// VISITOR design pattern
	@Override
	public void visit(NullValue nv) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(OrExpression oe) {
		visitBinaryOperation(oe);

		final Predicate right = _predStack.pop();
		final Predicate left = _predStack.pop();

		final Predicate or = new OrPredicate(left, right);
		_predStack.push(or);
	}

	@Override
	public void visit(Parenthesis prnths) {
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
		visitBinaryOperation(s);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ValueExpression sub = new ch.epfl.data.squall.expressions.Subtraction(
				left, right);
		_exprStack.push(sub);
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
