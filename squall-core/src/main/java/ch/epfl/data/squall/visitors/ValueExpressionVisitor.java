package ch.epfl.data.squall.visitors;

import ch.epfl.data.squall.expressions.Addition;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.DateDiff;
import ch.epfl.data.squall.expressions.DateSum;
import ch.epfl.data.squall.expressions.Division;
import ch.epfl.data.squall.expressions.IntegerYearFromDate;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.StringConcatenate;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueSpecification;

public interface ValueExpressionVisitor {
	public void visit(Addition add);

	public void visit(ColumnReference cr);

	public void visit(DateDiff dd);

	public void visit(DateSum ds);

	public void visit(Division dvsn);

	public void visit(IntegerYearFromDate iyfd);

	public void visit(Multiplication mult);

	public void visit(StringConcatenate sc);

	public void visit(Subtraction sub);

	public void visit(ValueSpecification vs);
}
