package ch.epfl.data.sql.visitors.squall;

import java.beans.Expression;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.conversion.StringConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.Addition;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.Division;
import ch.epfl.data.plan_runner.expressions.Multiplication;
import ch.epfl.data.plan_runner.expressions.Subtraction;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.sql.optimizers.name.NameTranslator;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.util.TupleSchema;

public class NameSelectItemsVisitor extends IndexSelectItemsVisitor {
	private final NameTranslator _nt;

	private final TupleSchema _tupleSchema;

	private final static StringConversion _sc = new StringConversion();

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
		final TypeConversion tc = _sc;

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
