package sql.estimators;

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

    //any of these keeps _result = null
    public void visit(NullValue nv) {

    }

    public void visit(Function fnctn) {
        
    }

    public void visit(InverseExpression ie) {
        
    }

    public void visit(JdbcParameter jp) {
        
    }

    public void visit(TimeValue tv) {
        
    }

    public void visit(TimestampValue tv) {
        
    }

    public void visit(Parenthesis prnths) {
        
    }

    public void visit(Addition adtn) {
        
    }

    public void visit(Division dvsn) {
        
    }

    public void visit(Multiplication m) {
        
    }

    public void visit(Subtraction s) {
        
    }

    public void visit(AndExpression ae) {
        
    }

    public void visit(OrExpression oe) {
        
    }

    public void visit(Between btwn) {
        
    }

    public void visit(EqualsTo et) {
        
    }

    public void visit(GreaterThan gt) {
        
    }

    public void visit(GreaterThanEquals gte) {
        
    }

    public void visit(InExpression ie) {
        
    }

    public void visit(IsNullExpression ine) {
        
    }

    public void visit(LikeExpression le) {
        
    }

    public void visit(MinorThan mt) {
        
    }

    public void visit(MinorThanEquals mte) {
        
    }

    public void visit(NotEqualsTo net) {
        
    }

    public void visit(Column column) {
        
    }

    public void visit(SubSelect ss) {
        
    }

    public void visit(CaseExpression ce) {
        
    }

    public void visit(WhenClause wc) {
        
    }

    public void visit(ExistsExpression ee) {
        
    }

    public void visit(AllComparisonExpression ace) {
        
    }

    public void visit(AnyComparisonExpression ace) {
        
    }

    public void visit(Concat concat) {
        
    }

    public void visit(Matches mtchs) {
        
    }

    public void visit(BitwiseAnd ba) {
        
    }

    public void visit(BitwiseOr bo) {
        
    }

    public void visit(BitwiseXor bx) {
        
    }
}