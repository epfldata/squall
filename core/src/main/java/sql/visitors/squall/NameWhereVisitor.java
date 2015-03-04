package sql.visitors.squall;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;
import plan_runner.components.Component;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueExpression;
import sql.optimizers.name.NameTranslator;
import sql.util.ParserUtil;
import sql.util.TupleSchema;

public class NameWhereVisitor extends IndexWhereVisitor {
	private final NameTranslator _nt;

	private final TupleSchema _tupleSchema;

	public NameWhereVisitor(TupleSchema tupleSchema, Component affectedComponent) {
		_tupleSchema = tupleSchema;

		_nt = new NameTranslator(affectedComponent.getName());
	}

	/*
	 * returns true if an expression was found in tupleSchema true means no need
	 * to call parent It has side effects - putting on exprStack
	 */
	private <T extends Expression> boolean isRecognized(T expr) {
		// expr is changed in place, so that it does not contain synonims
		final int position = _nt.indexOf(_tupleSchema, expr);
		if (position != ParserUtil.NOT_FOUND) {
			// we found an expression already in the tuple schema
			final TypeConversion tc = _nt.getType(_tupleSchema, expr);
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
		// column might be changed, due to the synonim effect
		final int position = _nt.getColumnIndex(_tupleSchema, column);

		// extract type for the column
		final TypeConversion tc = _nt.getType(_tupleSchema, column);

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
		if (!isRecognized(function))
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