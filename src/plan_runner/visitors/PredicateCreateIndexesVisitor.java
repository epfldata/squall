package plan_runner.visitors;

import java.util.ArrayList;
import java.util.List;

import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.LikePredicate;
import plan_runner.predicates.OrPredicate;
import plan_runner.predicates.Predicate;
import plan_runner.thetajoin.indexes.BplusTreeIndex;
import plan_runner.thetajoin.indexes.HashIndex;
import plan_runner.thetajoin.indexes.Index;

public class PredicateCreateIndexesVisitor implements PredicateVisitor{

	public List<Index> _firstRelationIndexes = new ArrayList<Index>();
	public List<Index> _secondRelationIndexes = new ArrayList<Index>();
	
	public List<Integer> _operatorForIndexes = new ArrayList<Integer>();
	public List<Object> _typeOfValueIndexed = new ArrayList<Object>();
		
	
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
		
	}
	
	public void visit(Predicate pred) {
		pred.accept(this);
	}

    public void visit(LikePredicate like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
