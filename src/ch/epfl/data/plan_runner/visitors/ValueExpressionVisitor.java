package ch.epfl.data.plan_runner.visitors;

import ch.epfl.data.plan_runner.expressions.Addition;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.DateDiff;
import ch.epfl.data.plan_runner.expressions.DateSum;
import ch.epfl.data.plan_runner.expressions.Division;
import ch.epfl.data.plan_runner.expressions.IntegerYearFromDate;
import ch.epfl.data.plan_runner.expressions.Multiplication;
import ch.epfl.data.plan_runner.expressions.StringConcatenate;
import ch.epfl.data.plan_runner.expressions.Subtraction;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;

public interface ValueExpressionVisitor {
    public void visit(Addition add);

    public void visit(ColumnReference cr);

    public void visit(DateSum ds);

    public void visit(DateDiff dd);

    public void visit(Division dvsn);

    public void visit(IntegerYearFromDate iyfd);

    public void visit(Multiplication mult);

    public void visit(StringConcatenate sc);

    public void visit(Subtraction sub);

    public void visit(ValueSpecification vs);
}
