package visitors.squall;

import components.Component;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import operators.AggregateOperator;
import operators.ChainOperator;
import operators.DistinctOperator;
import operators.Operator;
import operators.ProjectOperator;
import operators.SelectOperator;
import predicates.Predicate;
import visitors.OperatorVisitor;

/*
 * Collects all the VE inside a component.
 *   Lists refering to VE appearing after and before projection are necessary only for rule-based optimization.
 */
public class VECollectVisitor implements OperatorVisitor {
    private List<ValueExpression> _veList = new ArrayList<ValueExpression>();
    private List<ValueExpression> _afterProjection = new ArrayList<ValueExpression>();
    private List<ValueExpression> _beforeProjection = new ArrayList<ValueExpression>();

    public List<ValueExpression> getAllExpressions() {
        return _veList;
    }

    public List<ValueExpression> getAfterProjExpressions(){
        return _afterProjection;
    }

    public List<ValueExpression> getBeforeProjExpressions(){
        return _beforeProjection;
    }

    public void visit(Component component) {
        List<ValueExpression> hashExpressions = component.getHashExpressions();
        if(hashExpressions != null){
            _afterProjection.addAll(hashExpressions);
            _veList.addAll(hashExpressions);
        }

        List<Operator> operators = component.getChainOperator().getOperators();
        for(Operator op: operators){
            op.accept(this);
        }
    }

    @Override
    public void visit(SelectOperator selection){
        Predicate predicate = selection.getPredicate();
        VECollectPredVisitor vecpv = new VECollectPredVisitor();
        predicate.accept(vecpv);
        _beforeProjection.addAll(vecpv.getExpressions());
        _veList.addAll(vecpv.getExpressions());
    }

    //TODO: this should be only in the last component
    @Override
    public void visit(AggregateOperator aggregation) {
        if(aggregation!=null){
            DistinctOperator distinct = aggregation.getDistinct();
            if(distinct!=null){
                visitNested(aggregation.getDistinct());
            }
            if(aggregation.getGroupByProjection()!=null){
                _afterProjection.addAll(aggregation.getGroupByProjection().getExpressions());
                _veList.addAll(aggregation.getGroupByProjection().getExpressions());
            }
            _afterProjection.addAll(aggregation.getExpressions());
            _veList.addAll(aggregation.getExpressions());
        }
    }
    
    //unsupported
    //because we assing by ourselves to projection
    @Override
    public void visit(ProjectOperator projection){
        //TODO ignored because of topDown - makes no harm
    }

    //because it changes the output of the component
    @Override
    public void visit(DistinctOperator distinct){
        throw new RuntimeException("EarlyProjection cannon work if in bottom-up phase encounter Distinct!");
    }

    @Override
    public void visit(ChainOperator chain){
        for(Operator op: chain.getOperators()){
            op.accept(this);
        }
    }

    private void visitNested(ProjectOperator projection) {
        if(projection!=null){
            _afterProjection.addAll(projection.getExpressions());
            _veList.addAll(projection.getExpressions());
        }
    }

    private void visitNested(DistinctOperator distinct) {
         ProjectOperator project = distinct.getProjection();
         if(project!=null){
            visitNested(project);
         }
    }
    
}