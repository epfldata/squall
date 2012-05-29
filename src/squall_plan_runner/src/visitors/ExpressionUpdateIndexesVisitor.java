package visitors;

import java.util.ArrayList;
import java.util.List;

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

public class ExpressionUpdateIndexesVisitor implements ValueExpressionVisitor{

	private List<String> _tuple;
	public ArrayList<String> _valuesToIndex;
	public ArrayList<Object> _typesOfValuesToIndex;
	

	public ExpressionUpdateIndexesVisitor(List<String> tuple){
		
		_tuple = tuple;
		_valuesToIndex = new ArrayList<String>();
		_typesOfValuesToIndex = new ArrayList<Object>();
	}
	
	@Override
	public void visit(Addition addition) {
		List<ValueExpression> l = addition.getInnerExpressions();
		for(int index=0; index<l.size(); index++){
			visit(l.get(index));
		}
	}


	@Override
	public void visit(ColumnReference colRef) {
		_valuesToIndex.add(_tuple.get(colRef.getColumnIndex()));
		_typesOfValuesToIndex.add(colRef.getType().getInitialValue());
	}


	@Override
	public void visit(DateSum dateSum) {
		List<ValueExpression> l = dateSum.getInnerExpressions();
		for(int index=0; index<l.size(); index++){
			visit(l.get(index));
		}
	}


	@Override
	public void visit(IntegerYearFromDate year) {
		List<ValueExpression> l = year.getInnerExpressions();
		for(int index=0; index<l.size(); index++){
			visit(l.get(index));
		}
	}


	@Override
	public void visit(Multiplication multiplication) {
		List<ValueExpression> l = multiplication.getInnerExpressions();
		for(int index=0; index<l.size(); index++){
			visit(l.get(index));
		}
	}

	@Override
	public void visit(Division division) {
		List<ValueExpression> l = division.getInnerExpressions();
		for(int index=0; index<l.size(); index++){
			visit(l.get(index));
		}
	}

	@Override
	public void visit(StringConcatenate stringConcatenate) {
		List<ValueExpression> l = stringConcatenate.getInnerExpressions();
		for(int index=0; index<l.size(); index++){
			visit(l.get(index));
		}
	}


	@Override
	public void visit(Subtraction substraction) {
		List<ValueExpression> l = substraction.getInnerExpressions();
		for(int index=0; index<l.size(); index++){
			visit(l.get(index));
		}
	}


	@Override
	public void visit(ValueSpecification valueSpecification) {
		List<ValueExpression> l = valueSpecification.getInnerExpressions();
		for(int index=0; index<l.size(); index++){
			visit(l.get(index));
		}
	}
	
	public void visit(ValueExpression expr){
		expr.accept(this);
	}

}
