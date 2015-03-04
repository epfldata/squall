package plan_runner.visitors;

import plan_runner.ewh.operators.SampleAsideAndForwardOperator;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ChainOperator;
import plan_runner.operators.DistinctOperator;
import plan_runner.operators.PrintOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SampleOperator;
import plan_runner.operators.SelectOperator;

public interface OperatorVisitor {

	public void visit(AggregateOperator aggregation);

	public void visit(ChainOperator chain);

	public void visit(DistinctOperator distinct);

	public void visit(ProjectOperator projection);

	public void visit(SelectOperator selection);

	public void visit(PrintOperator printOperator);

	public void visit(SampleOperator sampleOperator);

	public void visit(SampleAsideAndForwardOperator sampleAsideAndForwardOperator);

}
