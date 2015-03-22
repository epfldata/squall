package ch.epfl.data.sql.visitors.squall;

import java.beans.Expression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.conversion.DateConversion;
import ch.epfl.data.plan_runner.conversion.DoubleConversion;
import ch.epfl.data.plan_runner.conversion.LongConversion;
import ch.epfl.data.plan_runner.conversion.StringConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.Addition;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.Division;
import ch.epfl.data.plan_runner.expressions.IntegerYearFromDate;
import ch.epfl.data.plan_runner.expressions.Multiplication;
import ch.epfl.data.plan_runner.expressions.Subtraction;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateCountOperator;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.AggregateSumOperator;
import ch.epfl.data.plan_runner.operators.DistinctOperator;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.sql.optimizers.index.IndexTranslator;
import ch.epfl.data.sql.schema.Schema;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.util.TableAliasName;

/*
 * Generates Aggregations and its groupBy projections.
 *   If there is no aggregation, these groupBy projections becomes simple projections
 */
public class IndexSelectItemsVisitor implements SelectItemVisitor,
		ExpressionVisitor, ItemsListVisitor {
	// all of these needed only for visit(Column) method
	private Schema _schema;
	private Component _affectedComponent;
	private TableAliasName _tan;
	private IndexTranslator _it;

	// needed only for visit(Function) method
	private final Map _map;

	private Stack<ValueExpression> _exprStack;
	private AggregateOperator _agg = null;

	// these two are of interest for the invoker
	private final List<AggregateOperator> _aggOps = new ArrayList<AggregateOperator>();
	private final List<ValueExpression> _groupByVEs = new ArrayList<ValueExpression>();
	private final List<Expression> _groupByExprs = new ArrayList<Expression>();

	public static final int AGG = 0;
	public static final int NON_AGG = 1;

	// this will not break any contracts,
	// even with new DateConversion() on all the places,
	// we will have a single object per (possibly) multiple spout/bolt threads.
	// generating plans is done from a single thread, static additionally saves
	// space
	private static LongConversion _lc = new LongConversion();
	private static DoubleConversion _dblConv = new DoubleConversion();
	private static DateConversion _dateConv = new DateConversion();
	private static StringConversion _sc = new StringConversion();

	protected IndexSelectItemsVisitor(Map map) {
		_map = map;
	}

	public IndexSelectItemsVisitor(QueryBuilder queryPlan, Schema schema,
			TableAliasName tan, Map map) {
		_schema = schema;
		_tan = tan;
		_map = map;
		_affectedComponent = queryPlan.getLastComponent();

		_it = new IndexTranslator(_schema, _tan);
	}

	/*
	 * veList used only in DISTINCT mode
	 */
	protected void createCount(List<ValueExpression> veList, boolean isDistinct) {
		// COUNT(R.A) and COUNT(1) have the same semantics as COUNT(*), since we
		// do not have NULLs in R.A
		_agg = new AggregateCountOperator(_map);

		// DISTINCT and agg are stored on the same component.
		if (isDistinct) {
			final DistinctOperator distinct = new DistinctOperator(_map, veList);
			_agg.setDistinct(distinct);
		}
	}

	protected void createSum(ValueExpression ve, boolean isDistinct) {
		_agg = new AggregateSumOperator(ve, _map);

		// DISTINCT and agg are stored on the same component.
		if (isDistinct) {
			final DistinctOperator distinct = new DistinctOperator(_map, ve);
			_agg.setDistinct(distinct);
		}
	}

	private void doneSingleItem(Expression expr) {
		if (_agg == null) {
			_groupByExprs.add(expr);
			_groupByVEs.add(_exprStack.peek());
		} else
			_aggOps.add(_agg);
	}

	public List<AggregateOperator> getAggOps() {
		return _aggOps;
	}

	public List<Expression> getGroupByExprs() {
		return _groupByExprs;
	}

	public List<ValueExpression> getGroupByVEs() {
		return _groupByVEs;
	}

	protected ValueExpression popFromExprStack() {
		return _exprStack.pop();
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
		visitBinaryExpression(adtn);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ValueExpression ve = new ch.epfl.data.plan_runner.expressions.Addition(
				left, right);
		_exprStack.push(ve);
	}

	// SELECTITEMVISITOR DESIGN PATTERN
	@Override
	public void visit(AllColumns ac) {
		// i.e. SELECT * FROM R join S
		// we need not to do anything in this case for RuleOptimizer
		// TODO: support it for Cost-Optimizer (Now, each wanted column has to
		// explicitly specified)
	}

	@Override
	public void visit(AllComparisonExpression ace) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(AllTableColumns atc) {
		// i.e. SELECT R.* FROM R join S
		// this is very rare, so it's not supported
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
		// extract type for the column
		final TypeConversion tc = _schema.getType(ParserUtil
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
		visitBinaryExpression(dvsn);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ValueExpression ve = new ch.epfl.data.plan_runner.expressions.Division(
				left, right);
		_exprStack.push(ve);
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

	// my VISITOR methods
	@Override
	public void visit(Function function) {
		// all aggregate functions (SUM, AVG, COUNT, MAX, MIN) have only one
		// parameter (Expression)
		// although COUNT(*) has no parameters
		// EXTRACT_YEAR has one parameter
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
		if (fnName.equalsIgnoreCase("SUM")) {
			// there must be only one parameter, if not SQL parser will raise an
			// exception
			final ValueExpression expr = expressions.get(0);
			createSum(expr, function.isDistinct());
		} else if (fnName.equalsIgnoreCase("COUNT"))
			createCount(expressions, function.isDistinct());
		else if (fnName.equalsIgnoreCase("EXTRACT_YEAR")) {
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
		visitBinaryExpression(m);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ValueExpression ve = new ch.epfl.data.plan_runner.expressions.Multiplication(
				left, right);
		_exprStack.push(ve);
	}

	@Override
	public void visit(NotEqualsTo net) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	// EXPRESSIONVISITOR DESIGN PATTERN
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
		prnths.getExpression().accept(this);
	}

	@Override
	public void visit(SelectExpressionItem sei) {
		_exprStack = new Stack<ValueExpression>();
		_agg = null;
		final Expression expr = sei.getExpression();
		expr.accept(this);
		doneSingleItem(expr);
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
		visitBinaryExpression(s);

		final ValueExpression right = _exprStack.pop();
		final ValueExpression left = _exprStack.pop();

		final ValueExpression ve = new ch.epfl.data.plan_runner.expressions.Subtraction(
				left, right);
		_exprStack.push(ve);

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

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		binaryExpression.getLeftExpression().accept(this);
		binaryExpression.getRightExpression().accept(this);
	}

}
