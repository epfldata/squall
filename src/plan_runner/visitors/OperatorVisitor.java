package plan_runner.visitors;

import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.DistinctOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;


public interface OperatorVisitor {

    public void visit(SelectOperator selection);

    public void visit(DistinctOperator distinct);

    public void visit(ProjectOperator projection);

    public void visit(AggregateOperator aggregation);
    
    public void visit(ChainOperator chain);

}
