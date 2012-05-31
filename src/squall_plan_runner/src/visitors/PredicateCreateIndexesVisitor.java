package visitors;

import java.util.ArrayList;
import java.util.List;

import expressions.ValueExpression;


import predicates.AndPredicate;
import predicates.BetweenPredicate;
import predicates.ComparisonPredicate;
import predicates.LikePredicate;
import predicates.OrPredicate;
import predicates.Predicate;
import thetajoin.indexes.BplusTreeIndex;
import thetajoin.indexes.HashIndex;
import thetajoin.indexes.Index;

public class PredicateCreateIndexesVisitor implements PredicateVisitor{

	public List<Index> _firstRelationIndexes = new ArrayList<Index>();
	public List<Index> _secondRelationIndexes = new ArrayList<Index>();
	
	public List<Integer> _operatorForIndexes = new ArrayList<Integer>();
	public List<Object> _typeOfValueIndexed = new ArrayList<Object>();
	
	
	//The indexes of the column useful for the join
	//stock in the array where we interleaves the indexes of R and S : [R.u, S.v, R.m, S.n, ...]
	public List<Integer> _colsRef = new ArrayList<Integer>();

	
	//For the cases where
	// the join condition is an equality/inequality expression (of the form R.0 = a * S.1 + b)
	// on integer or double
	//stock the coefficients of a and b for each conditions 
	public List<Object> _coefA = new ArrayList<Object>();
	public List<Object> _coefB = new ArrayList<Object>();
	
	
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
		_operatorForIndexes.add(comparison.getOperation());
		_typeOfValueIndexed.add(comparison.getType());
		
		boolean isString = false;
		System.out.println("visitComp");
		if (comparison.getOperation()==ComparisonPredicate.EQUAL_OP){
			if(comparison.getType() instanceof Integer){
				_firstRelationIndexes.add(new HashIndex<Integer>());
				_secondRelationIndexes.add(new HashIndex<Integer>());
			}else if(comparison.getType() instanceof Double){
				_firstRelationIndexes.add(new HashIndex<Double>());
				_secondRelationIndexes.add(new HashIndex<Double>());
			}else if(comparison.getType() instanceof String){
				_firstRelationIndexes.add(new HashIndex<String>());
				_secondRelationIndexes.add(new HashIndex<String>());
				isString =true;
			}else{
				throw new RuntimeException("non supported type");
			}
		}else{
			if(comparison.getType() instanceof Integer){
				_firstRelationIndexes.add(new BplusTreeIndex<Integer>(3,2));
				_secondRelationIndexes.add(new BplusTreeIndex<Integer>(3,2));
			}else if(comparison.getType() instanceof Double){
				_firstRelationIndexes.add(new BplusTreeIndex<Double>(3,2));
				_secondRelationIndexes.add(new BplusTreeIndex<Double>(3,2));
			}else if(comparison.getType() instanceof String){
				_firstRelationIndexes.add(new BplusTreeIndex<String>(3,2));
				_secondRelationIndexes.add(new BplusTreeIndex<String>(3,2));
				isString =true;
			}else{
				throw new RuntimeException("non supported type");
			}
		}
		
		if(!isString){
			ExpressionCreateIndexesVisitor vi = new ExpressionCreateIndexesVisitor();
			//We don't need to visit this branch for finding a and b coeficients as we have the form : R.0 op a * S.1 + b
			((ValueExpression)comparison.getExpressions().get(0)).accept(vi);
			((ValueExpression)comparison.getExpressions().get(1)).accept(vi);
			
			if(comparison.getOperation() == ComparisonPredicate.EQUAL_OP){
				_colsRef.add(vi._R);
				_colsRef.add(vi._S);
			}
			
			if(vi._a != null && vi._b != null){
				_coefA.add(vi._a);
				_coefB.add(vi._b);
			}
		}else{
			if(comparison.getOperation() == ComparisonPredicate.EQUAL_OP){
				ExpressionCreateIndexesVisitor vi = new ExpressionCreateIndexesVisitor();
				((ValueExpression)comparison.getExpressions().get(0)).accept(vi);
				_colsRef.add(vi._R);
				
				((ValueExpression)comparison.getExpressions().get(1)).accept(vi);
				_colsRef.add(vi._R);
			}
			
		}
		
		
		
	}
	
	public void visit(Predicate pred) {
		System.out.println("visit");
		pred.accept(this);
	}

    public void visit(LikePredicate like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
