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

public class PredicateUpdateIndexesVisitor implements PredicateVisitor, ExpressionVisitor{

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
		visit(between.getInnerPredicates().get(0));
	}

	@Override
	public void visit(ComparisonPredicate comparison) {
		if(_comeFromFirstEmitter){
			visit(comparison.getExpressions().get(0));
		}else{
			visit(comparison.getExpressions().get(1));
		}
		
	}

	
	public void visit(Object object) {
		try
		{
			Method downPolymorphic = object.getClass().getMethod("visit",
				new Class[] { object.getClass() });

			if (downPolymorphic == null) {
				defaultVisit(object);
			} else {
				downPolymorphic.invoke(this, new Object[] {object});
			}
		}
		catch (NoSuchMethodException e)
		{
			this.defaultVisit(object);
		}
		catch (InvocationTargetException e)
		{
			this.defaultVisit(object);
		}   
		catch (IllegalAccessException e)
		{
			this.defaultVisit(object);
		}      	
	}
	
	public void defaultVisit(Object object)
	{
		// if we don't know the class we do nothing
		if (object.getClass().equals(Predicate.class) || object.getClass().equals(ValueExpression.class))
		{
			System.out.println("default visit: "
				+ object.getClass().getSimpleName());
		}
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

}
