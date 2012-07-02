/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors;

import expressions.*;


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
