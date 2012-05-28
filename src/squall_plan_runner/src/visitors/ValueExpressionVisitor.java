/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors;

import expressions.Addition;
import expressions.ColumnReference;
import expressions.DateSum;
import expressions.Division;
import expressions.IntegerYearFromDate;
import expressions.Multiplication;
import expressions.StringConcatenate;
import expressions.Subtraction;
import expressions.ValueSpecification;


public interface ValueExpressionVisitor {
    public void visit(Addition add);
    public void visit(ColumnReference cr);
    public void visit(DateSum ds);
    public void visit(IntegerYearFromDate iyfd);
    public void visit(Multiplication mult);
    public void visit(Division dvsn);
    public void visit(StringConcatenate sc);
    public void visit(Subtraction sub);
    public void visit(ValueSpecification vs);
}
