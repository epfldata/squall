package estimators;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/*
 * this class extracts Java types objects out of JSQL wrapper objects
 */
public class JSQLTypeConverter implements ExpressionVisitor{

    private Object _result;

    public void visit(DateValue dv){
        _result = dv.getValue();
    }
            
    public void visit(DoubleValue dv){
        _result = dv.getValue();
    }

    public void visit(LongValue lv){
        _result = lv.getValue();
    }

    public void visit(StringValue sv){
        _result = sv.getValue();
    }

    public Object getResult(){
        return _result;
    }

    //UNUSED
    public void visit(NullValue nv) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Function fnctn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(InverseExpression ie) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(JdbcParameter jp) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(TimeValue tv) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(TimestampValue tv) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Parenthesis prnths) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Addition adtn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Division dvsn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Multiplication m) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Subtraction s) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(AndExpression ae) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(OrExpression oe) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Between btwn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(EqualsTo et) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(GreaterThan gt) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(GreaterThanEquals gte) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(InExpression ie) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(IsNullExpression ine) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(LikeExpression le) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(MinorThan mt) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(MinorThanEquals mte) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(NotEqualsTo net) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Column column) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(SubSelect ss) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(CaseExpression ce) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(WhenClause wc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(ExistsExpression ee) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(AllComparisonExpression ace) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(AnyComparisonExpression ace) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Concat concat) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(Matches mtchs) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(BitwiseAnd ba) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(BitwiseOr bo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void visit(BitwiseXor bx) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
