package ch.epfl.data.sql.visitors.jsql;

import java.beans.Expression;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import ch.epfl.data.plan_runner.expressions.Addition;
import ch.epfl.data.plan_runner.expressions.Division;
import ch.epfl.data.plan_runner.expressions.Multiplication;
import ch.epfl.data.plan_runner.expressions.Subtraction;
import ch.epfl.data.sql.util.JoinTablesExprs;
import ch.epfl.data.sql.util.TableAliasName;

public class SQLVisitor implements SelectVisitor, FromItemVisitor,
		ExpressionVisitor, ItemsListVisitor, Serializable {
	private static Logger LOG = Logger.getLogger(SQLVisitor.class);
	private static final long serialVersionUID = 1L;

	private List<Table> _tableList;
	private List<Join> _joinList;
	private List<SelectItem> _selectItems;
	private Expression _whereExpr;

	private final String _queryName;

	private TableAliasName _tan;
	private JoinTablesExprs _jte;

	public SQLVisitor(String queryName) {
		_queryName = queryName;
	}

	public void doneVisiting() {
		// fill in tan
		_tan = new TableAliasName(_tableList, _queryName);

		// create JoinTableExpr
		// From a list of joins, create collection of elements like {R->{S,
		// R.A=S.A}}
		final JoinTablesExprsVisitor jteVisitor = new JoinTablesExprsVisitor();
		for (final Join join : _joinList)
			join.getOnExpression().accept(jteVisitor);
		_jte = jteVisitor.getJoinTablesExp();
	}

	public JoinTablesExprs getJte() {
		return _jte;
	}

	public List<SelectItem> getSelectItems() {
		return _selectItems;
	}

	public List<Table> getTableList() {
		return _tableList;
	}

	public TableAliasName getTan() {
		return _tan;
	}

	public Expression getWhereExpr() {
		return _whereExpr;
	}

	@Override
	public void visit(Addition addition) {
		visitBinaryExpression(addition);
	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {
		allComparisonExpression.GetSubSelect().getSelectBody().accept(this);
	}

	@Override
	public void visit(AndExpression andExpression) {
		visitBinaryExpression(andExpression);
	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		anyComparisonExpression.GetSubSelect().getSelectBody().accept(this);
	}

	@Override
	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		visitBinaryExpression(bitwiseAnd);
	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
		visitBinaryExpression(bitwiseOr);
	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
		visitBinaryExpression(bitwiseXor);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser
	 * .expression.CaseExpression)
	 */
	@Override
	public void visit(CaseExpression caseExpression) {
	}

	// empty
	@Override
	public void visit(Column tableColumn) {
	}

	@Override
	public void visit(Concat concat) {
		visitBinaryExpression(concat);
	}

	@Override
	public void visit(DateValue dateValue) {
	}

	@Override
	public void visit(Division division) {
		visitBinaryExpression(division);
	}

	@Override
	public void visit(DoubleValue doubleValue) {
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		visitBinaryExpression(equalsTo);
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		existsExpression.getRightExpression().accept(this);
	}

	@Override
	public void visit(ExpressionList expressionList) {
		LOG.info("ExprList!");
		for (final Iterator iter = expressionList.getExpressions().iterator(); iter
				.hasNext();) {
			final Expression expression = (Expression) iter.next();
			expression.accept(this);
		}

	}

	@Override
	public void visit(Function function) {
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		visitBinaryExpression(greaterThan);
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		visitBinaryExpression(greaterThanEquals);
	}

	@Override
	public void visit(InExpression inExpression) {
		inExpression.getLeftExpression().accept(this);
		inExpression.getItemsList().accept(this);
	}

	@Override
	public void visit(InverseExpression inverseExpression) {
		inverseExpression.getExpression().accept(this);
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {
	}

	public void visit(Join join) {
		_joinList.add(join);
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		visitBinaryExpression(likeExpression);
	}

	@Override
	public void visit(LongValue longValue) {
	}

	@Override
	public void visit(Matches matches) {
		visitBinaryExpression(matches);
	}

	@Override
	public void visit(MinorThan minorThan) {
		visitBinaryExpression(minorThan);
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		visitBinaryExpression(minorThanEquals);
	}

	@Override
	public void visit(Multiplication multiplication) {
		visitBinaryExpression(multiplication);
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		visitBinaryExpression(notEqualsTo);
	}

	@Override
	public void visit(NullValue nullValue) {
	}

	@Override
	public void visit(OrExpression orExpression) {
		visitBinaryExpression(orExpression);
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}

	// VISITOR DESIGN PATTERN
	@Override
	public void visit(PlainSelect plainSelect) {
		plainSelect.getFromItem().accept(this);

		_selectItems.addAll(plainSelect.getSelectItems());

		if (plainSelect.getJoins() != null)
			for (final Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt
					.hasNext();) {
				final Join join = (Join) joinsIt.next();
				visit(join);
				join.getRightItem().accept(this);
			}
		_whereExpr = plainSelect.getWhere();
		if (_whereExpr != null)
			_whereExpr.accept(this);
	}

	// CUSTOM METHODS
	public void visit(Select select) {
		_tableList = new ArrayList<Table>();
		_joinList = new ArrayList<Join>();
		_selectItems = new ArrayList<SelectItem>();
		_whereExpr = null;
		select.getSelectBody().accept(this);
	}

	@Override
	public void visit(StringValue stringValue) {
	}

	@Override
	public void visit(SubJoin subjoin) {
		LOG.info("Subjoin!");
		subjoin.getLeft().accept(this);
		subjoin.getJoin().getRightItem().accept(this);
	}

	@Override
	public void visit(SubSelect subSelect) {
		LOG.info("Subselect!");
		subSelect.getSelectBody().accept(this);
	}

	@Override
	public void visit(Subtraction subtraction) {
		visitBinaryExpression(subtraction);
	}

	@Override
	public void visit(Table table) {
		_tableList.add(table);
	}

	@Override
	public void visit(TimestampValue timestampValue) {
	}

	@Override
	public void visit(TimeValue timeValue) {
	}

	@Override
	public void visit(Union union) {
		for (final Iterator iter = union.getPlainSelects().iterator(); iter
				.hasNext();) {
			final PlainSelect plainSelect = (PlainSelect) iter.next();
			visit(plainSelect);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser
	 * .expression.WhenClause)
	 */
	@Override
	public void visit(WhenClause whenClause) {
	}

	private void visitBinaryExpression(BinaryExpression binaryExpression) {
		binaryExpression.getLeftExpression().accept(this);
		binaryExpression.getRightExpression().accept(this);
	}

}