package visitors.jsql;

import java.util.Iterator;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;
import util.JoinTablesExprs;
import util.ParserUtil;

/*
 * This class contains information regarding all the possible joins between any two tables.
 *   It is used when we want to find out whether two Components can be joined or not
 *   (the necessary condition is that they have at least one R.A = S.B,
 *    where R(S) is one of the ancestors of the first(second) component).
 *
 * This class populates joinTableExp from join conditition expressions.
 *
 * We don't hash immediately on all the columns which will be used for all the joins below in the hierarchy.
 * For example X=(R join S), Y=(T join V), and we want X join Y.
 *   Then, we hash X on columns S.C and S.D in expressions S.C=T.C and S.D=V.D
 *   R might be later joined with fifth table.
 * Thus, sending information about all the tables R joins with in JoinHashVisitor, will result in hashing on unnecessary columns.
 *
 * Let's take a look at a join condition: R.A = S.A and T.B = S.B
 * On both sides must be at least one relation (otherwise it is WHERE clause)
 *    and at most one relation - we need it since we index joins by tables keys,
 *    so R.A + S.A = 5 is not supported right now (TODO)
 * Returns two objects {R->{S, exp{R.A = S.A}} and {T->{S, exp{T.B=S.B}}
 *
 * TODO: OR in join condition is not yet supported.
 */
public class JoinTablesExprsVisitor implements ExpressionVisitor, ItemsListVisitor {
    private Table _sideTable;
    private JoinTablesExprs _joinTablesExp = new JoinTablesExprs();

    public JoinTablesExprs getJoinTablesExp(){
        return _joinTablesExp;
    }

    @Override
    public void visit(AndExpression ae) {
        visitBinaryOperation(ae);
    }

    @Override
    public void visit(EqualsTo et) {
        Table leftTable = visitSideEquals(et.getLeftExpression());
        Table rightTable = visitSideEquals(et.getRightExpression());
        _joinTablesExp.addEntry(leftTable, rightTable, et);
    }

    private Table visitSideEquals(Expression ex){
        _sideTable = null;
        ex.accept(this);
        if(_sideTable == null){
            throw new RuntimeException("At least one table must appear in Join condition!");
        }
        return _sideTable;
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

    private void visitBinaryOperation(BinaryExpression be){
        be.getLeftExpression().accept(this);
        be.getRightExpression().accept(this);
    }

    @Override
    public void visit(Parenthesis prnths) {
        prnths.getExpression().accept(this);
    }

    @Override
    public void visit(Function function) {
        //all aggregate functions (SUM, AVG, COUNT, MAX, MIN) have only one parameter (Expression)
        //although COUNT(*) has no parameters
        //EXTRACT_YEAR has one parameter
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
        Table affectedTable = column.getTable();
        if(_sideTable == null){
            _sideTable = affectedTable;
        }else{
            if(!ParserUtil.equals(_sideTable, affectedTable)){
                throw new RuntimeException("Multiple tables on one side of a join condition is not supported yet!");
            }
        }
    }

    //all of ValueSpecifications (constants) guarantee we have some expressions in join conditions
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

    //UNSUPPORTED
    @Override
    public void visit(OrExpression oe) {
        throw new RuntimeException("OR in join condition is not yet supported!");
    }

    @Override
    public void visit(GreaterThan gt) {
        visitUnsupportedOp();
    }

    @Override
    public void visit(GreaterThanEquals gte) {
        visitUnsupportedOp();
    }

    @Override
    public void visit(MinorThan mt) {
        visitUnsupportedOp();
    }

    @Override
    public void visit(MinorThanEquals mte) {
        visitUnsupportedOp();
    }

    @Override
    public void visit(NotEqualsTo net) {
        visitUnsupportedOp();
    }

    private void visitUnsupportedOp(){
        throw new RuntimeException("Only EQUALS operator can appear in join condition!");
    }

    //VISITOR DESIGN PATTERN
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