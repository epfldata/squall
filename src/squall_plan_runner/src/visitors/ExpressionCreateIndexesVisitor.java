package visitors;

import expressions.Addition;
import expressions.ColumnReference;
import expressions.DateSum;
import expressions.Division;
import expressions.IntegerYearFromDate;
import expressions.Multiplication;
import expressions.StringConcatenate;
import expressions.Subtraction;
import expressions.ValueExpression;
import expressions.ValueSpecification;

public class ExpressionCreateIndexesVisitor implements ValueExpressionVisitor{

	public Object _a = null;
	public Object _b = null;
	public int _S;
	public int _R;
	
	@Override
	public void visit(Addition addition) {
		ValueExpression exp0 = (ValueExpression) addition.getInnerExpressions().get(0);
		ValueExpression exp1 = (ValueExpression) addition.getInnerExpressions().get(1);
		if (exp0 instanceof Multiplication && exp1 instanceof ValueSpecification){
			visit(exp0);
			visit(exp1);
		}
		
	}

	@Override
	public void visit(ColumnReference colRef) {
		_R = colRef.getColumnIndex();
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
		ValueExpression exp0 = (ValueExpression) multiplication.getInnerExpressions().get(0);
		ValueExpression exp1 = (ValueExpression) multiplication.getInnerExpressions().get(1);
		if (exp0 instanceof ValueSpecification && exp1 instanceof ColumnReference){
			_a = ((ValueSpecification)exp0).eval(null);
			_S = ((ColumnReference)exp1).getColumnIndex();
		}
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
		_b = valueSpecification.eval(null);		
	}
	
	public void visit(ValueExpression ex) {
		ex.accept(this);
	}

}
