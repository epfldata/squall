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


package ch.epfl.data.squall.api.sql.estimators;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import ch.epfl.data.squall.api.sql.schema.Schema;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.util.TableAliasName;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.Type;

/* TODO high prio:
 * no matter on which component we do invoke, the only important is to know previous projections
 *   (TPCH7, 8 with pushing OR from WHERE clause)
 *
 * TODO low prio: ERROR MESSAGE
 * do not support R.A + 4 < 2, we will need ValueExpression with ranges to support that, and a user can rewrite it itself
 * do not support R.A + R.B = 2 (no exception)
 *   if there are multiple fields addressed, they have probably some dependency, which we don't model yet
 */
public class SelingerSelectivityEstimator implements SelectivityEstimator {

	private final String _queryName;
	private final Schema _schema;
	private final TableAliasName _tan;

	public SelingerSelectivityEstimator(String queryName, Schema schema,
			TableAliasName tan) {
		_queryName = queryName;
		_schema = schema;
		_tan = tan;
	}

	private Long doubleToLong(Double doubleValue) {
		final Long result = doubleValue.longValue();
		if (result.doubleValue() != doubleValue)
			throw new RuntimeException(
					"Rounding error! Check your schema file.");
		return result;
	}

	public double estimate(AndExpression and) {
		// the case when we have the same single column on both sides
		final Expression leftExpr = and.getLeftExpression();
		final List<Column> leftColumns = ParserUtil.getJSQLColumns(leftExpr);
		final Column leftColumn = leftColumns.get(0);

		final Expression rightExpr = and.getRightExpression();
		final List<Column> rightColumns = ParserUtil.getJSQLColumns(rightExpr);
		final Column rightColumn = rightColumns.get(0);

		if (leftColumn.toString().equals(rightColumn.toString()))
			// not using leftExpr and rightExpr, because we want to preserve
			// type
			return 1 - (1 - estimate(and.getLeftExpression()))
					- (1 - estimate(and.getRightExpression()));
		else
			return estimate(and.getLeftExpression())
					* estimate(and.getRightExpression());

	}

	public double estimate(EqualsTo equals) {
		final List<Column> columns = ParserUtil.getJSQLColumns(equals);

		final Column column = columns.get(0);
		final String fullSchemaColumnName = _tan
				.getFullSchemaColumnName(column);

		final long distinctValues = _schema
				.getNumDistinctValues(fullSchemaColumnName);
		return 1.0 / distinctValues;
	}

	@Override
	public double estimate(Expression expr) {
		// similarly to JSQLTypeConvertor, it can be done via visitor pattern,
		// but then it cannot implement SelectivityEstimator anymore.
		// the gap between void of visit method and double as the result here
		// can be solved in a similar manner as in JSQLTypeConverter.
		if (expr instanceof EqualsTo)
			return estimate((EqualsTo) expr);
		else if (expr instanceof NotEqualsTo)
			return estimate((NotEqualsTo) expr);
		else if (expr instanceof MinorThan)
			return estimate((MinorThan) expr);
		else if (expr instanceof MinorThanEquals)
			return estimate((MinorThanEquals) expr);
		else if (expr instanceof GreaterThan)
			return estimate((GreaterThan) expr);
		else if (expr instanceof GreaterThanEquals)
			return estimate((GreaterThanEquals) expr);
		else if (expr instanceof AndExpression)
			return estimate((AndExpression) expr);
		else if (expr instanceof OrExpression)
			return estimate((OrExpression) expr);
		else if (expr instanceof Parenthesis) {
			final Parenthesis pnths = (Parenthesis) expr;
			return estimate(pnths.getExpression());
		} else
			return HardCodedSelectivities.estimate(_queryName, expr);
	}

	public double estimate(GreaterThan gt) {
		final EqualsTo equals = new EqualsTo();
		equals.setLeftExpression(gt.getLeftExpression());
		equals.setRightExpression(gt.getRightExpression());

		final MinorThan minorThan = new MinorThan();
		minorThan.setLeftExpression(gt.getLeftExpression());
		minorThan.setRightExpression(gt.getRightExpression());

		return 1 - estimate(equals) - estimate(minorThan);
	}

	public double estimate(GreaterThanEquals gt) {
		final MinorThan minorThan = new MinorThan();
		minorThan.setLeftExpression(gt.getLeftExpression());
		minorThan.setRightExpression(gt.getRightExpression());

		return 1 - estimate(minorThan);
	}

	public double estimate(List<Expression> exprs) {
		// this is treated as a list of AndExpressions
		if (exprs.size() == 1)
			return estimate(exprs.get(0));

		// at least two expressions in the list
		AndExpression and = new AndExpression(exprs.get(0), exprs.get(1));
		for (int i = 2; i < exprs.size(); i++)
			and = new AndExpression(and, exprs.get(i));
		return estimate(and);
	}

	public double estimate(MinorThan mt) {
		final List<Column> columns = ParserUtil.getJSQLColumns(mt);
		final Column column = columns.get(0);
		final Type tc = _schema.getType(ParserUtil
				.getFullSchemaColumnName(column, _tan));

		// TODO: assume uniform distribution
		final String fullSchemaColumnName = _tan
				.getFullSchemaColumnName(column);
		Object minValue = _schema.getRange(fullSchemaColumnName).getMin();
		Object maxValue = _schema.getRange(fullSchemaColumnName).getMax();

		// We have to compare the same types
		if (tc instanceof DoubleType) {
			if (minValue instanceof Long)
				minValue = longToDouble((Long) minValue);
			if (maxValue instanceof Long)
				maxValue = longToDouble((Long) maxValue);
		} else if (tc instanceof LongType) {
			if (minValue instanceof Double)
				minValue = doubleToLong((Double) minValue);
			if (maxValue instanceof Double)
				maxValue = doubleToLong((Double) maxValue);
		}

		final double fullRange = tc.getDistance(maxValue, minValue);
		final Expression leftExp = mt.getLeftExpression();
		final Expression rightExp = mt.getRightExpression();

		Object conditionConstant = findConditionConstant(rightExp);
		if (conditionConstant == null)
			// maybe the constant is on the left side
			conditionConstant = findConditionConstant(leftExp);

		if (conditionConstant != null) {
			// a constant on one side
			// MAKE TPCH-6 WORK WITH NCL OPTIMIZER
			if (tc instanceof DoubleType) {
				if (conditionConstant instanceof Long)
					conditionConstant = longToDouble((Long) conditionConstant);
			} else if (tc instanceof LongType)
				if (conditionConstant instanceof Double)
					conditionConstant = doubleToLong((Double) conditionConstant);
			final double distance = tc.getDistance(conditionConstant, minValue);
			return distance / fullRange;
		} else
			// no constants on both sides; columns within a single table are
			// compared
			return HardCodedSelectivities.estimate(_queryName, mt);
	}

	public double estimate(MinorThanEquals mte) {
		final EqualsTo equals = new EqualsTo();
		equals.setLeftExpression(mte.getLeftExpression());
		equals.setRightExpression(mte.getRightExpression());

		final MinorThan minorThan = new MinorThan();
		minorThan.setLeftExpression(mte.getLeftExpression());
		minorThan.setRightExpression(mte.getRightExpression());

		return estimate(minorThan) + estimate(equals);
	}

	/*
	 * computed using the basic ones (= and <)
	 */
	public double estimate(NotEqualsTo ne) {
		final EqualsTo equals = new EqualsTo();
		equals.setLeftExpression(ne.getLeftExpression());
		equals.setRightExpression(ne.getRightExpression());

		return 1 - estimate(equals);
	}

	/*
	 * And, Or expressions
	 */
	public double estimate(OrExpression or) {
		return estimate(or.getLeftExpression())
				+ estimate(or.getRightExpression());
	}

	/*
	 * WHERE R.A < 4, or WHERE 4 < R.A, This method returns 4.
	 */
	private Object findConditionConstant(Expression exp) {
		final JSQLTypeConverter converter = new JSQLTypeConverter();
		exp.accept(converter);
		final Object currentValue = converter.getResult();
		return currentValue;
	}

	private Double longToDouble(Long longValue) {
		return longValue.doubleValue();
	}

}
