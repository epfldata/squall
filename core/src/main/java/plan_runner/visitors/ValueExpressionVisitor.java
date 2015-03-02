package plan_runner.visitors;

import plan_runner.expressions.Addition;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.DateDiff;
import plan_runner.expressions.DateSum;
import plan_runner.expressions.Division;
import plan_runner.expressions.IntegerYearFromDate;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.StringConcatenate;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueSpecification;

public interface ValueExpressionVisitor {
	public void visit(Addition add);

	public void visit(ColumnReference cr);

	public void visit(DateSum ds);

	public void visit(DateDiff dd);	

	public void visit(Division dvsn);

	public void visit(IntegerYearFromDate iyfd);

	public void visit(Multiplication mult);

	public void visit(StringConcatenate sc);

	public void visit(Subtraction sub);

	public void visit(ValueSpecification vs);
}
