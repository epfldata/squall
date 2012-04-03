/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors.squall;

import components.Component;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import operators.AggregateOperator;
import operators.DistinctOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import predicates.Predicate;

//VECollectVisitor is meant to visit all the inside of the component in search for VEs
public class VECollectVisitor {
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

        SelectionOperator selection = component.getSelection();
        if(selection!=null){
            visit(selection);
        }
        ProjectionOperator projection = component.getProjection();
        if(projection!=null){
            visit(projection);
        }
        DistinctOperator distinct = component.getDistinct();
        if(distinct!=null){
            visit(distinct);
        }
        AggregateOperator aggregation = component.getAggregation();
        if(aggregation!=null){
            visit(aggregation);
        }
    }

    public void visit(SelectionOperator selection){
        Predicate predicate = selection.getPredicate();
        VECollectPredVisitor vecpv = new VECollectPredVisitor();
        predicate.accept(vecpv);
        _beforeProjection.addAll(vecpv.getExpressions());
        _veList.addAll(vecpv.getExpressions());
    }

    private void visitNested(ProjectionOperator projection) {
        if(projection!=null){
            _afterProjection.addAll(projection.getExpressions());
            _veList.addAll(projection.getExpressions());
        }
    }

    private void visitNested(DistinctOperator distinct) {
         ProjectionOperator project = distinct.getProjection();
         if(project!=null){
            visitNested(project);
         }
    }

    //TODO: this should be only in the last component
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
    public void visit(ProjectionOperator projection){
        //TODO ignored because of topDown - makes no harm
    }

    //because it changes the output of the component
    public void visit(DistinctOperator distinct){
        throw new RuntimeException("EarlyProjection cannon work if in bottom-up phase encounter Distinct!");
    }
    
}