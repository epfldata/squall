package sql.visitors.jsql;

import java.util.Iterator;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import sql.util.ParserUtil;

/*
 * PrintUser is the conventional way to comparing expressions in tuple schemas
 *   Used in NameTranslator
 */
public class PrintVisitor implements ExpressionVisitor, ItemsListVisitor {
    StringBuilder _sb = new StringBuilder();

    public String getString(){
        String result = _sb.toString();
        _sb = new StringBuilder();
        return result;
    }

    @Override
    public void visit(Function function) {
        _sb.append(function.getName());
        ExpressionList params = function.getParameters();
        if(params != null){
            if(function.isDistinct()){
                _sb.append("(DISTINCT ");
            }else{
                _sb.append("(");
            }
            visit(params);
            _sb.append(")");
        }else{
            _sb.append("()");
        }
    }

    @Override
    public void visit(ExpressionList expressionList) {
        for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
            if (iter.hasNext()){
                _sb.append(", ");
            }
        }
    }

    @Override
    public void visit(Column column) {
        _sb.append(ParserUtil.getFullAliasedName(column));
    }    

    @Override
    public void visit(Addition adtn) {
        visitBinaryExpression(adtn, " + ");
    }

    @Override
    public void visit(Division dvsn) {
        visitBinaryExpression(dvsn, " / ");
    }

    @Override
    public void visit(Multiplication m) {
        visitBinaryExpression(m, " * ");
    }

    @Override
    public void visit(Subtraction s) {
        visitBinaryExpression(s, " - ");
    }
    
    @Override
    public void visit(AndExpression ae) {
        visitBinaryExpression(ae, " AND ");
    }

    @Override
    public void visit(OrExpression oe) {
        visitBinaryExpression(oe, " OR ");
    }
    
    @Override
    public void visit(EqualsTo et) {
        visitBinaryExpression(et, " = ");
    }    

    @Override
    public void visit(DoubleValue dv) {
        _sb.append(dv.getValue());
    }

    @Override
    public void visit(LongValue lv) {
        _sb.append(lv.getValue());
    }

    @Override
    public void visit(DateValue dv) {
        _sb.append(dv.getValue());
    }

    @Override
    public void visit(StringValue sv) {
        _sb.append(sv.getValue());
    }

    @Override
    public void visit(Parenthesis prnths) {
        _sb.append("(");
        prnths.getExpression().accept(this);
        _sb.append(")");
    }

    //private visitor methods
    private void visitBinaryExpression(BinaryExpression binaryExpression, String operator) {
        binaryExpression.getLeftExpression().accept(this);
        _sb.append(operator);
        binaryExpression.getRightExpression().accept(this);
    }



    //not used
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
    public void visit(GreaterThan gt) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(GreaterThanEquals gte) {
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
    public void visit(MinorThan mt) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(MinorThanEquals mte) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(NotEqualsTo net) {
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