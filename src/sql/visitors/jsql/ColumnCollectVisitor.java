package sql.visitors.jsql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;


public class ColumnCollectVisitor implements ExpressionVisitor, ItemsListVisitor {

    private List<Column> _listColumns = new ArrayList<Column>();

    public List<Column> getColumns(){
        return _listColumns;
    }

    @Override
    public void visit(AndExpression ae) {
        visitBinaryOperation(ae);
    }

    @Override
    public void visit(OrExpression oe) {
        visitBinaryOperation(oe);
    }

    @Override
    public void visit(Addition adtn) {
        visitBinaryOperation(adtn);
    }

    @Override
    public void visit(Multiplication m) {
        visitBinaryOperation(m);
    }

    @Override
    public void visit(Division dvsn) {
        visitBinaryOperation(dvsn);
    }

    @Override
    public void visit(Subtraction s) {
        visitBinaryOperation(s);
    }

    @Override
    public void visit(EqualsTo et) {
        visitBinaryOperation(et);
    }

    @Override
    public void visit(LikeExpression le) {
        visitBinaryOperation(le);
    }

    @Override
    public void visit(GreaterThan gt) {
        visitBinaryOperation(gt);
    }

    @Override
    public void visit(GreaterThanEquals gte) {
        visitBinaryOperation(gte);
    }


    @Override
    public void visit(MinorThan mt) {
        visitBinaryOperation(mt);
    }

    @Override
    public void visit(MinorThanEquals mte) {
        visitBinaryOperation(mte);
    }

    @Override
    public void visit(NotEqualsTo net) {
        visitBinaryOperation(net);
    }

    private void visitBinaryOperation(BinaryExpression be){
        be.getLeftExpression().accept(this);
        be.getRightExpression().accept(this);
    }

    @Override
    public void visit(Function function) {
        ExpressionList params = function.getParameters();
        if(params != null){
            visit(params);
        }
    }

    @Override
    public void visit(ExpressionList el) {
        for (Iterator iter = el.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
        }
    }

    @Override
    public void visit(Column column) {
        _listColumns.add(column);
    }

    @Override
    public void visit(DoubleValue dv) {

    }

    @Override
    public void visit(LongValue lv) {

    }

    @Override
    public void visit(DateValue dv) {

    }

    @Override
    public void visit(StringValue sv) {

    }

    @Override
    public void visit(Parenthesis prnths) {
        prnths.getExpression().accept(this);
    }

    //VISITOR design pattern
    @Override
    public void visit(NullValue nv) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(InverseExpression ie) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(JdbcParameter jp) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(TimeValue tv) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(TimestampValue tv) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(Between btwn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(InExpression ie) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(IsNullExpression ine) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(SubSelect ss) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(CaseExpression ce) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(WhenClause wc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(ExistsExpression ee) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(AllComparisonExpression ace) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(AnyComparisonExpression ace) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(Concat concat) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(Matches mtchs) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(BitwiseAnd ba) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(BitwiseOr bo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(BitwiseXor bx) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
