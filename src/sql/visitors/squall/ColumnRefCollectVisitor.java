package sql.visitors.squall;

import java.util.ArrayList;
import java.util.List;

import plan_runner.expressions.Addition;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.DateSum;
import plan_runner.expressions.Division;
import plan_runner.expressions.IntegerYearFromDate;
import plan_runner.expressions.Multiplication;
import plan_runner.expressions.StringConcatenate;
import plan_runner.expressions.Subtraction;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import plan_runner.visitors.ValueExpressionVisitor;

public class ColumnRefCollectVisitor implements ValueExpressionVisitor {
	private final List<ColumnReference> _crList = new ArrayList<ColumnReference>();

	public List<ColumnReference> getColumnRefs() {
		return _crList;
	}

	@Override
	public void visit(Addition add) {
		visit(add.getInnerExpressions());
	}

	@Override
	public void visit(ColumnReference cr) {
		_crList.add(cr);
	}

	@Override
	public void visit(DateSum ds) {
		visit(ds.getInnerExpressions());
	}

	@Override
	public void visit(Division dvsn) {
		visit(dvsn.getInnerExpressions());
	}

	@Override
	public void visit(IntegerYearFromDate iyfd) {
		visit(iyfd.getInnerExpressions());
	}

	private void visit(List<ValueExpression> veList) {
		for (final ValueExpression ve : veList)
			ve.accept(this);
	}

	@Override
	public void visit(Multiplication mult) {
		visit(mult.getInnerExpressions());
	}

	@Override
	public void visit(StringConcatenate sc) {
		visit(sc.getInnerExpressions());
	}

	@Override
	public void visit(Subtraction sub) {
		visit(sub.getInnerExpressions());
	}

	@Override
	public void visit(ValueSpecification vs) {
		// constant
	}

}
