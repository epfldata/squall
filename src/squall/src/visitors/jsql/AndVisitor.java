package visitors.jsql;

import java.util.ArrayList;
import java.util.List;
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
 * Extracts all the conjunctive terms
 * (R.A=3) and ( (S.A = 3 and R.A =4)or (S.A = 4 and R.A = 3))
 */
public class AndVisitor implements ExpressionVisitor {

    private List<Expression> _atomicExprs = new ArrayList<Expression>();
    private List<OrExpression> _orExprs = new ArrayList<OrExpression>();

    public List<Expression> getAtomicExprs(){
        return _atomicExprs;
    }
    
    public List<OrExpression> getOrExprs(){
        return _orExprs;
    }

    @Override
    public void visit(AndExpression ae) {
        Expression left = ae.getLeftExpression();
        Expression right = ae.getRightExpression();

        visitAndSide(left);
        visitAndSide(right);
    }

    private void visitAndSide(Expression sideExpr){
        if(sideExpr instanceof AndExpression
                || sideExpr instanceof OrExpression
                || sideExpr instanceof Parenthesis){
            sideExpr.accept(this);
        }else {
            //everything else is an atomic condition
            _atomicExprs.add(sideExpr);
        }
    }

    @Override
    public void visit(OrExpression oe) {
        _orExprs.add(oe);
    }

    @Override
    public void visit(Parenthesis prnths) {
        prnths.getExpression().accept(this);
    }

    //we might arrive here directly
    @Override
    public void visit(EqualsTo et) {
        _atomicExprs.add(et);
    }

    @Override
    public void visit(GreaterThan gt) {
        _atomicExprs.add(gt);
    }

    @Override
    public void visit(GreaterThanEquals gte) {
        _atomicExprs.add(gte);
    }

    @Override
    public void visit(MinorThan mt) {
        _atomicExprs.add(mt);
    }

    @Override
    public void visit(MinorThanEquals mte) {
        _atomicExprs.add(mte);
    }

    @Override
    public void visit(NotEqualsTo net) {
        _atomicExprs.add(net);
    }

    //not necessary for this stage
    @Override
    public void visit(Addition adtn) {
        
    }

    @Override
    public void visit(Multiplication m) {
        
    }

    @Override
    public void visit(Subtraction s) {
        
    }

    @Override
    public void visit(Function function) {
        
    }

    @Override
    public void visit(Column column) {
        
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
    public void visit(Division dvsn) {
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
    public void visit(LikeExpression le) {
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