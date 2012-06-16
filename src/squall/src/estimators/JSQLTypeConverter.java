package estimators;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
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

    //just for testing purposes
    public static void main(String[] args){
        Expression exp = new DateValue("d" + "1995-01-01" + "d");
        JSQLTypeConverter visitor = new JSQLTypeConverter();
        exp.accept(visitor);
        System.out.println(visitor.getResult());
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
