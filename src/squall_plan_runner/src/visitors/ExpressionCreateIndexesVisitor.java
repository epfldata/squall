package visitors;

import expressions.Addition;
import expressions.ColumnReference;
import expressions.DateSum;
import expressions.Division;
import expressions.IntegerYearFromDate;
import expressions.Multiplication;
import expressions.StringConcatenate;
import expressions.Subtraction;
import expressions.ValueSpecification;

public class ExpressionCreateIndexesVisitor implements ValueExpressionVisitor{

	@Override
	public void visit(Addition addition) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ColumnReference colRef) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateSum dateSum) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IntegerYearFromDate year) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication multiplication) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division division) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringConcatenate stringConcatenate) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction substraction) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ValueSpecification valueSpecification) {
		// TODO Auto-generated method stub
		
	}

}
