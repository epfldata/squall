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


package ch.epfl.data.squall.api.sql.optimizers.name;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import ch.epfl.data.squall.api.sql.schema.ColumnNameType;
import ch.epfl.data.squall.api.sql.schema.Schema;
import ch.epfl.data.squall.api.sql.util.JoinTablesExprs;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.util.TableAliasName;
import ch.epfl.data.squall.api.sql.util.TupleSchema;
import ch.epfl.data.squall.api.sql.visitors.jsql.MaxSubExpressionsVisitor;
import ch.epfl.data.squall.api.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.squall.api.sql.visitors.squall.NameProjectVisitor;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.conversion.IntegerConversion;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ProjectOperator;

/*
 * This class takes expressions from GlobalProjExpr,
 *   add to them those from Hashes,
 *   and create an output schema based on which expressions are required down the topology.
 * Correctly recognized subexpressions as well.
 */
public class ProjSchemaCreator {
	private final ProjGlobalCollect _globalProject; // this is shared by all the
	// ProjSchemaCreator objects
	private final TupleSchema _inputTupleSchema;

	private final NameTranslator _nt;
	private final TableAliasName _tan; // used for getting a list of all the
	// tableCompNames
	private final Schema _schema;
	private final JoinTablesExprs _jte; // used for getting joinCondition
	private final Component _component;
	private final SQLVisitor _pq;
	private final NameProjectVisitor _npv;

	// output of this class
	private TupleSchema _outputTupleSchema;
	private List<ValueExpression> _veList;

	private static final IntegerConversion _ic = new IntegerConversion();

	public ProjSchemaCreator(ProjGlobalCollect globalProject,
			TupleSchema inputTupleSchema, Component component, SQLVisitor pq,
			Schema schema) {

		_globalProject = globalProject;
		_inputTupleSchema = inputTupleSchema;
		_tan = pq.getTan();
		_schema = schema;
		_jte = pq.getJte();
		_component = component;
		_pq = pq;

		_nt = new NameTranslator(component.getName());
		_npv = new NameProjectVisitor(_inputTupleSchema, component);
	}

	/*
	 * Expressions from exprList are all appeapring somewhere in the query plan
	 * This method never can raise an exception, it can only cause suboptimality
	 * There is no mandatory projections because SelectOperator,
	 * AggregateOperator and Hashes are able to deal with ValueExpressions (and
	 * not only with ColumnReferences). For example, if I have inputTupleSchema
	 * "R.A, R.A + R.B" in R and exprList "R.A, R.A + R.B" and decide to go with
	 * outputTupleSchema "R.A, R.B", there is no "R.B" in inputTupleSchema
	 * However, this is not possible, because parent will never send something
	 * like this "R.A, R.A + R.B".
	 */
	private List<Expression> chooseProjections(List<Expression> exprList) {
		// colect all the columnNames from JSQL Column Expressions
		final List<String> aloneColumnNames = getAloneColumnNames(exprList);

		List<Expression> resultExpr = new ArrayList<Expression>();
		for (final Expression expr : exprList)
			if (expr instanceof Column)
				resultExpr.add(expr); // all the Column Expressions should be
			// added
			else {
				// all the columns used in expression expr
				final List<Column> exprColumns = ParserUtil
						.getJSQLColumns(expr);
				boolean existsAlone = false; // does at least one column from
				// expr already appears in
				// aloneColumnNames?
				for (final Column column : exprColumns) {
					final String columnStr = ParserUtil.getStringExpr(column);
					if (aloneColumnNames != null
							&& aloneColumnNames.contains(columnStr)) {
						existsAlone = true;
						break;
					}
				}
				if (existsAlone)
					// all the columns should be added to result
					resultExpr.addAll(exprColumns);
				else
					// add whole expr
					resultExpr.add(expr);
			}

		// now take care of duplicates
		resultExpr = eliminateDuplicates(resultExpr);

		return resultExpr;
	}

	/*
	 * can be invoked multiple times with no harm
	 */
	public void create() {
		final List<Expression> exprList = new ArrayList<Expression>();

		// these methods adds to exprList
		// each added expression is either present in inputTupleSchema, or can
		// be built out of it
		processGlobalExprs(exprList);
		processGlobalOrs(exprList);
		if (!ParserUtil.isFinalComponent(_component, _pq))
			// last component does not have hashes, because it's joined with
			// noone
			processHashes(exprList);

		// choose for which expressions we do projection, and create a schema
		// out of that
		final List<Expression> chosenExprs = chooseProjections(exprList);
		_outputTupleSchema = createSchema(chosenExprs);

		// convert JSQL to Squall expressions
		_npv.visit(chosenExprs);
		_veList = _npv.getExprs();

	}

	/*
	 * Create new schema, but preserve all the synonims from _inputTupleSchema
	 */
	private TupleSchema createSchema(List<Expression> choosenExprs) {
		final List<ColumnNameType> cnts = new ArrayList<ColumnNameType>();

		for (final Expression expr : choosenExprs) {
			// first to determine the type, we use the first column for that

			final TypeConversion tc = getTC(expr);

			// attach the TypeConversion
			final String exprStr = ParserUtil.getStringExpr(expr);
			final ColumnNameType cnt = new ColumnNameType(exprStr, tc);
			cnts.add(cnt);
		}

		// copying all the synonims from inputTupleSchema
		final TupleSchema result = new TupleSchema(cnts);
		final Map<String, String> inputSynonims = _inputTupleSchema
				.getSynonims();
		if (inputSynonims != null)
			result.setSynonims(inputSynonims);

		return result;
	}

	/*
	 * We have to convert it to String, because that's the way we implemented
	 * equals operator (we don't want to change JSQL classes to add equals
	 * operator)
	 */
	private List<Expression> eliminateDuplicates(List<Expression> exprList) {
		final List<Expression> result = new ArrayList<Expression>();
		final List<String> exprStrList = new ArrayList<String>();
		for (final Expression expr : exprList) {
			final String exprStr = ParserUtil.getStringExpr(expr);
			if (!exprStrList.contains(exprStr))
				// if it is not already there, add it
				result.add(expr);
			// anyway we update a list of strings
			exprStrList.add(exprStr);
		}
		return result;
	}

	/*
	 * Return all the columns which appears alone in its String form e.g. for
	 * R(A), R(A) + 5, R(B), R(C) + 2 this methods return R(A), R(B)
	 */
	private List<String> getAloneColumnNames(List<Expression> exprList) {
		final List<String> result = new ArrayList<String>();
		for (final Expression expr : exprList)
			if (expr instanceof Column) {
				final Column column = (Column) expr;
				result.add(ParserUtil.getStringExpr(column));
			}
		return result;
	}

	public TupleSchema getOutputSchema() {
		return _outputTupleSchema;
	}

	/*
	 * will be used for a creation of a ProjectOperator
	 */
	public ProjectOperator getProjectOperator() {
		return new ProjectOperator(_veList);
	}

	/*
	 * Have to distinguish special cases from normal ones
	 */
	private TypeConversion getTC(Expression expr) {
		if (expr instanceof Function) {
			final Function fun = (Function) expr;
			if (fun.getName().equalsIgnoreCase("EXTRACT_YEAR"))
				return _ic;
		}

		// non special cases
		final List<Column> columns = ParserUtil.getJSQLColumns(expr);
		final Column column = columns.get(0);
		return _schema
				.getType(ParserUtil.getFullSchemaColumnName(column, _tan));
	}

	/*
	 * For each expression from _globalProject (for now these are only from
	 * SELECT clause), add the appropriate subexpressions to _exprList
	 */
	private void processGlobalExprs(List<Expression> exprList) {
		final MaxSubExpressionsVisitor sev = new MaxSubExpressionsVisitor(_nt,
				_inputTupleSchema);
		sev.visit(_globalProject.getExprList());
		exprList.addAll(sev.getExprs());
	}

	/*
	 * OrExpressions are from WHERE clause We need to project for it if they are
	 * not already executed This could work without subexpressions, because
	 * SelectOperator can work with ValueExpressions (and not only with
	 * ColumnReferences)
	 */
	private void processGlobalOrs(List<Expression> exprList) {
		final List<OrExpression> orList = _globalProject.getOrExprs();
		if (orList != null)
			for (final OrExpression orExpr : _globalProject.getOrExprs()) {
				final MaxSubExpressionsVisitor sev = new MaxSubExpressionsVisitor(
						_nt, _inputTupleSchema);
				sev.visit(orExpr);
				if (!sev.isAllSubsMine(orExpr)) {
					// if all of them are available, SELECT operator is already
					// done
					// (either in this component because SELECT goes before
					// PROJECT
					// or in some of ancestor components)

					// we get all the subexpressions correlated to me
					final List<Expression> mineSubExprs = sev.getExprs();
					exprList.addAll(mineSubExprs);
				}
			}
	}

	/*
	 * All the HashExpressions for joinining between ancestor of component and
	 * all other tables are collected
	 */
	private void processHashes(List<Expression> exprList) {
		final List<String> ancestorNames = ParserUtil
				.getSourceNameList(_component);

		// it has to be done like this, because queryPlan is not finished
		// and does not contain all the tables yet
		final List<String> allCompNames = _tan.getComponentNames();
		final List<String> otherCompNames = ParserUtil.getDifference(
				allCompNames, ancestorNames);

		// now we find joinCondition between ancestorNames and otherCompNames
		// joinExprs is a list of EqualsTo
		final List<Expression> joinExprs = _jte.getExpressions(ancestorNames,
				otherCompNames);

		final MaxSubExpressionsVisitor sev = new MaxSubExpressionsVisitor(_nt,
				_inputTupleSchema);
		sev.visit(joinExprs);
		// we get all the subexpressions correlated to me
		final List<Expression> mineSubExprs = sev.getExprs();
		exprList.addAll(mineSubExprs);

	}

}