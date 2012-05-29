/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors.squall;

import expressions.Addition;
import expressions.ColumnReference;
import expressions.DateSum;
import expressions.Division;
import expressions.IntegerYearFromDate;
import expressions.Multiplication;
import expressions.StringConcatenate;
import expressions.Subtraction;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.ArrayList;
import java.util.List;
import visitors.ValueExpressionVisitor;


public class ColumnRefCollectVisitor implements ValueExpressionVisitor{
    private List<ColumnReference> _crList = new ArrayList<ColumnReference>();
    
    public List<ColumnReference> getColumnRefs(){
        return _crList;
    }

    @Override
    public void visit(Addition add) {
        visit(add.getInnerExpressions());
    }

    @Override
    public void visit(DateSum ds) {
        visit(ds.getInnerExpressions());
    }

    @Override    
    public void visit(IntegerYearFromDate iyfd) {
        visit(iyfd.getInnerExpressions());
    }

    @Override    
    public void visit(Multiplication mult) {
        visit(mult.getInnerExpressions());
    }

    @Override    
    public void visit(Division dvsn) {
        visit(dvsn.getInnerExpressions());
    }
    
    @Override
    public void visit(StringConcatenate sc) {
        visit(sc.getInnerExpressions());
    }

    @Override
    public void visit(Subtraction sub) {
        visit(sub.getInnerExpressions());
    }

    private void visit(List<ValueExpression> veList){
        for(ValueExpression ve: veList){
            ve.accept(this);
        }
    }

    @Override
    public void visit(ColumnReference cr) {
        _crList.add(cr);
    }

    @Override
    public void visit(ValueSpecification vs) {
        //constant
    }

}
