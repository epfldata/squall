package ch.epfl.data.sql.estimators;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
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
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/*
 * this class extracts Java types objects out of JSQL wrapper objects
 */
public class JSQLTypeConverter implements ExpressionVisitor {

	private Object _result;

	public Object getResult() {
		return _result;
	}

	@Override
	public void visit(Addition adtn) {

	}

	@Override
	public void visit(AllComparisonExpression ace) {

	}

	@Override
	public void visit(AndExpression ae) {

	}

	@Override
	public void visit(AnyComparisonExpression ace) {

	}

	@Override
	public void visit(Between btwn) {

	}

	@Override
	public void visit(BitwiseAnd ba) {

	}

	@Override
	public void visit(BitwiseOr bo) {

	}

	@Override
	public void visit(BitwiseXor bx) {

	}

	@Override
	public void visit(CaseExpression ce) {

	}

	@Override
	public void visit(Column column) {

	}

	@Override
	public void visit(Concat concat) {

	}

	@Override
	public void visit(DateValue dv) {
		_result = dv.getValue();
	}

	@Override
	public void visit(Division dvsn) {

	}

	@Override
	public void visit(DoubleValue dv) {
		_result = dv.getValue();
	}

	@Override
	public void visit(EqualsTo et) {

	}

	@Override
	public void visit(ExistsExpression ee) {

	}

	@Override
	public void visit(Function fnctn) {

	}

	@Override
	public void visit(GreaterThan gt) {

	}

	@Override
	public void visit(GreaterThanEquals gte) {

	}

	@Override
	public void visit(InExpression ie) {

	}

	@Override
	public void visit(InverseExpression ie) {

	}

	@Override
	public void visit(IsNullExpression ine) {

	}

	@Override
	public void visit(JdbcParameter jp) {

	}

	@Override
	public void visit(LikeExpression le) {

	}

	@Override
	public void visit(LongValue lv) {
		_result = lv.getValue();
	}

	@Override
	public void visit(Matches mtchs) {

	}

	@Override
	public void visit(MinorThan mt) {

	}

	@Override
	public void visit(MinorThanEquals mte) {

	}

	@Override
	public void visit(Multiplication m) {

	}

	@Override
	public void visit(NotEqualsTo net) {

	}

	// any of these keeps _result = null
	@Override
	public void visit(NullValue nv) {

	}

	@Override
	public void visit(OrExpression oe) {

	}

	@Override
	public void visit(Parenthesis prnths) {

	}

	@Override
	public void visit(StringValue sv) {
		_result = sv.getValue();
	}

	@Override
	public void visit(SubSelect ss) {

	}

	@Override
	public void visit(Subtraction s) {

	}

	@Override
	public void visit(TimestampValue tv) {

	}

	@Override
	public void visit(TimeValue tv) {

	}

	@Override
	public void visit(WhenClause wc) {

	}
}