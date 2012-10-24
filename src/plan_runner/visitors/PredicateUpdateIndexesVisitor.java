package plan_runner.visitors;

import java.util.ArrayList;
import java.util.List;
import plan_runner.expressions.ValueExpression;
import plan_runner.predicates.AndPredicate;
import plan_runner.predicates.BetweenPredicate;
import plan_runner.predicates.ComparisonPredicate;
import plan_runner.predicates.LikePredicate;
import plan_runner.predicates.OrPredicate;
import plan_runner.predicates.Predicate;

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
	public void visit(ComparisonPredicate comparison){
		ValueExpression val;
		
		if(_comeFromFirstEmitter){
			val = (ValueExpression) comparison.getExpressions().get(0);
		}else{
			val = (ValueExpression) comparison.getExpressions().get(1);
		}
		
		_valuesToIndex.add(val.eval(_tuple).toString());
		_typesOfValuesToIndex.add(val.getType().getInitialValue());
	}
	
	
	public void visit(Predicate pred) {
		pred.accept(this);
	}

    public void visit(LikePredicate like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
	
}
