package visitors;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import expressions.Addition;
import expressions.ColumnReference;
import expressions.DateSum;
import expressions.IntegerYearFromDate;
import expressions.Multiplication;
import expressions.StringConcatenate;
import expressions.Subtraction;
import expressions.ValueExpression;
import expressions.ValueSpecification;

import predicates.AndPredicate;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;
import predicates.OrPredicate;
import predicates.Predicate;

public class PredicateUpdateIndexesVisitor implements PredicateVisitor{

	private List<String> _tuple;
	
	private boolean _comeFromFirstEmitter;
	
	public ArrayList<String> _valuesToIndex;
	public ArrayList<Object> _typesOfValuesToIndex;
	
	public PredicateUpdateIndexesVisitor( boolean comeFromFirstEmitter,
			List<String> tuple){
		_comeFromFirstEmitter = comeFromFirstEmitter;
		_tuple = tuple;
		
		_valuesToIndex = new ArrayList<String>();
		_typesOfValuesToIndex = new ArrayList<Object>();
	}
	
	
	@Override
	public void visit(AndPredicate and) {
		for(Predicate pred : and.getInnerPredicates()){
			visit(pred);
		}
	}
	

	@Override
	public void visit(OrPredicate or) {
		for(Predicate pred : or.getInnerPredicates()){
			visit(pred);
		}
	}

	@Override
	public void visit(BetweenPredicate between) {
		//In between there is only an and predicate
		Predicate p = (Predicate)between.getInnerPredicates().get(0);
		visit(p);
	}

	@Override
	public void visit(ComparisonPredicate comparison) {
		ExpressionUpdateIndexesVisitor exprVisitor =
			new ExpressionUpdateIndexesVisitor(_tuple);
		
		ValueExpression val;
		
		if(_comeFromFirstEmitter){
			val = (ValueExpression) comparison.getExpressions().get(0);
		}else{
			val = (ValueExpression) comparison.getExpressions().get(1);
		}
		
		val.accept(exprVisitor);
		_valuesToIndex.addAll(exprVisitor._valuesToIndex);
		_typesOfValuesToIndex.addAll(exprVisitor._typesOfValuesToIndex);		
	}

	public void visit(Predicate pred) {
		pred.accept(this);
	}
	
}
