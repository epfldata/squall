package visitors;

import java.util.ArrayList;
import java.util.List;


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
	
	
	//For the cases where
	// the join condition is an equality/inequality expression (of the form R.0 = a * S.1 + b)
	// on integer or double
	//stock the coefficients of a and b for each conditions in the array of this form : [a, b, a, b, ...].
	
	
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
		
		//if(!isString)
		//visit(comparison.getExpressions().get(0));
		//visit(comparison.getExpressions().get(1));
		
	}
	
	public void visit(Predicate pred) {
		System.out.println("visit");
		pred.accept(this);
	}

    public void visit(LikePredicate like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
