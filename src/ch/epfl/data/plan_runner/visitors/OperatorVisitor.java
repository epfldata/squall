package ch.epfl.data.plan_runner.visitors;

import ch.epfl.data.plan_runner.ewh.operators.SampleAsideAndForwardOperator;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.DistinctOperator;
import ch.epfl.data.plan_runner.operators.PrintOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SampleOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;

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
