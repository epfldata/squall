package ch.epfl.data.squall.api.sql.visitors.jsql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

/*
 * This class return a list of (sub)expressions which corresponds to visited expressions
 *   AND can be built out of inputTupleSchema
 * For example, if visited expression is LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT)
 *   and input schema is (LINEITEM.EXTENDEDPRICE, 1.0 - LINEITEM.DISCOUNT)
 *   _exprList will consist of a single expression LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT)
 * Other example: If in R component we have two expressions in inpuTupleSchema: "R.A + 4, 50",
 *   this class will return "R.A + 4" expression
 * Used in ProjSchemaCreator
 */
public class MaxSubExpressionsVisitor implements ExpressionVisitor,
		ItemsListVisitor {
	private final NameTranslator _nt;
	private final TupleSchema _inputTupleSchema;
	private final List<Expression> _exprList = new ArrayList<Expression>();

	public MaxSubExpressionsVisitor(NameTranslator nt,
			TupleSchema inputTupleSchema) {
		_nt = nt;
		_inputTupleSchema = inputTupleSchema;
	}

	public List<Expression> getExprs() {
		return _exprList;
	}

	/*
	 * This returns true if expr is available in inputTupleSchema, or if all of
	 * its subexpressions are availabe in inputTupleSchema so that expr can be
	 * built out of subexpressions and constants
	 */
	public boolean isAllSubsMine(Expression expr) {
		if (_nt.contains(_inputTupleSchema, expr))
			return true;

		// unrecognized Column
		if (expr instanceof Column)
			return false;

		final List<Expression> subExprs = ParserUtil.getSubExpressions(expr);
		if (subExprs == null)
			// constants - it can be built from my inputTupleSchema
			return true;

		// for all other we have to check
		for (final Expression subExpr : subExprs)
			if (!isAllSubsMine(subExpr))
				return false;

		// all of subexpressions are mine
		return true;
	}

	private boolean isRecognized(Expression expr) {
		if (isAllSubsMine(expr)) {
			// if the same expression exists in inputTupleSchema, add it to the
			// output schema
			_exprList.add(expr);
			return true;
		} else
			return false;
	}

	@Override
	public void visit(Addition adtn) {
		if (!isRecognized(adtn))
			visitBinaryOp(adtn);
	}

	@Override
	public void visit(AllComparisonExpression ace) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	// this is for WHERE clause and HASH
	@Override
	public void visit(AndExpression ae) {
		visitBinaryOp(ae);
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
		// no subexpression recognized, still we add only mine columns
		if (_nt.contains(_inputTupleSchema, column))
			_exprList.add(column);
	}

	@Override
	public void visit(Concat concat) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(DateValue dv) {
		// if not recognized as a part of a subexpression, ignore it
	}

	@Override
	public void visit(Division dvsn) {
		if (!isRecognized(dvsn))
			visitBinaryOp(dvsn);
	}

	@Override
	public void visit(DoubleValue dv) {
		// if not recognized as a part of a subexpression, ignore it
	}

	@Override
	public void visit(EqualsTo et) {
		visitBinaryOp(et);
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
	public void visit(Function fnctn) {
		if (!isRecognized(fnctn))
			visit(fnctn.getParameters());
	}

	@Override
	public void visit(GreaterThan gt) {
		visitBinaryOp(gt);
	}

	@Override
	public void visit(GreaterThanEquals gte) {
		visitBinaryOp(gte);
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
		visitBinaryOp(le);
	}

	public void visit(List<Expression> inputExprList) {
		for (final Expression expr : inputExprList)
			expr.accept(this);
	}

	@Override
	public void visit(LongValue lv) {
		// if not recognized as a part of a subexpression, ignore it
	}

	@Override
	public void visit(Matches mtchs) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(MinorThan mt) {
		visitBinaryOp(mt);
	}

	@Override
	public void visit(MinorThanEquals mte) {
		visitBinaryOp(mte);
	}

	@Override
	public void visit(Multiplication m) {
		if (!isRecognized(m))
			visitBinaryOp(m);
	}

	@Override
	public void visit(NotEqualsTo net) {
		visitBinaryOp(net);
	}

	// not used
	@Override
	public void visit(NullValue nv) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(OrExpression oe) {
		visitBinaryOp(oe);
	}

	@Override
	public void visit(Parenthesis prnths) {
		if (!isRecognized(prnths))
			prnths.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue sv) {
		// if not recognized as a part of a subexpression, ignore it
	}

	@Override
	public void visit(SubSelect ss) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void visit(Subtraction s) {
		if (!isRecognized(s))
			visitBinaryOp(s);
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

	private void visitBinaryOp(BinaryExpression be) {
		be.getLeftExpression().accept(this);
		be.getRightExpression().accept(this);
	}

}