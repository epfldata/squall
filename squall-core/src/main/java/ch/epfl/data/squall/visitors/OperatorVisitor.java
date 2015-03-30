package ch.epfl.data.squall.visitors;

import ch.epfl.data.squall.ewh.operators.SampleAsideAndForwardOperator;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.DistinctOperator;
import ch.epfl.data.squall.operators.PrintOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SampleOperator;
import ch.epfl.data.squall.operators.SelectOperator;

public interface OperatorVisitor {

	public void visit(AggregateOperator aggregation);

	public void visit(ChainOperator chain);

	public void visit(DistinctOperator distinct);

	public void visit(PrintOperator printOperator);

	public void visit(ProjectOperator projection);

	public void visit(
			SampleAsideAndForwardOperator sampleAsideAndForwardOperator);

	public void visit(SampleOperator sampleOperator);

	public void visit(SelectOperator selection);

}
