/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors;

import operators.AggregateOperator;
import operators.DistinctOperator;
import operators.Operator;
import operators.ProjectOperator;
import operators.SelectOperator;


public interface OperatorVisitor {

    public void visit(SelectOperator selection);

    public void visit(DistinctOperator distinct);

    public void visit(ProjectOperator projection);

    public void visit(AggregateOperator aggregation);
    
    public void visit(Operator operator);

}
