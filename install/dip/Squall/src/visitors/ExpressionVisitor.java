package visitors;

import expressions.Addition;
import expressions.ColumnReference;
import expressions.DateSum;
import expressions.IntegerYearFromDate;
import expressions.Multiplication;
import expressions.StringConcatenate;
import expressions.Subtraction;
import expressions.ValueExpression;
import expressions.ValueSpecification;


public interface ExpressionVisitor {
	
	public void visit(Addition addition);
	public void visit(ColumnReference colRef);
	public void visit(DateSum dateSum);
	public void visit(IntegerYearFromDate year);
	public void visit(Multiplication multiplication);
	public void visit(StringConcatenate stringConcatenate);
	public void visit(Subtraction substraction);
	public void visit(ValueSpecification valueSpecification);	
}
