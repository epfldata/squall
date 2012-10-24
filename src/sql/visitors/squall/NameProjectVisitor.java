
package sql.visitors.squall;

import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import plan_runner.components.Component;
import plan_runner.conversion.*;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.IntegerYearFromDate;
import plan_runner.expressions.ValueExpression;
import plan_runner.expressions.ValueSpecification;
import sql.optimizers.name.NameTranslator;
import sql.util.ParserUtil;
import sql.util.TupleSchema;

/*
 * Used for Cost-optimizer(nameTranslator)
 *   to convert a list of JSQL expressions to a list of Squall ValueExpressions
 * Similar to IndexWhereVisitor, but does not use predStack
 */
public class NameProjectVisitor implements ExpressionVisitor, ItemsListVisitor{
    private final NameTranslator _nt;

    private final TupleSchema _inputTupleSchema;
    private Stack<ValueExpression> _exprStack = new Stack<ValueExpression>();

    //this will not break any contracts,
    //  even with new DateConversion() on all the places,
    //  we will have a single object per (possibly) multiple spout/bolt threads.
    //generating plans is done from a single thread, static additionally saves space
    private static LongConversion _lc = new LongConversion();
    private static DoubleConversion _dblConv = new DoubleConversion();
    private static DateConversion _dateConv = new DateConversion();
    private static StringConversion _sc = new StringConversion();

    public NameProjectVisitor(TupleSchema inputTupleSchema, Component affectedComponent){
        _inputTupleSchema = inputTupleSchema;
        
        _nt = new NameTranslator(affectedComponent.getName());
    }

    public List<ValueExpression> getExprs(){
        List<ValueExpression> veList = new ArrayList<ValueExpression>();
        while(!_exprStack.isEmpty()){
            veList.add(_exprStack.pop());
        }
        Collections.reverse(veList); // stack inverse the order of elements
        return veList;
    }

    private void pushToExprStack(ValueExpression ve){
        _exprStack.push(ve);
    }

    public void visit(List<Expression> exprs) {
        for(Expression expr: exprs){
            expr.accept(this);
        }
    }

    @Override
    public void visit(Column column) {
        //extract the position (index) of the required column
        //column might be changed, due to the synonim effect
        int position = _nt.getColumnIndex(_inputTupleSchema, column);

        //extract type for the column
        TypeConversion tc = _nt.getType(_inputTupleSchema, column);
        
        ValueExpression ve = new ColumnReference(tc, position, ParserUtil.getStringExpr(column));
        pushToExprStack(ve);
    }

    @Override
    public void visit(DoubleValue dv) {
        ValueExpression ve = new ValueSpecification(_dblConv, dv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(LongValue lv) {
        ValueExpression ve = new ValueSpecification(_lc, lv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(DateValue dv) {
        ValueExpression ve = new ValueSpecification(_dateConv, dv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(StringValue sv) {
        ValueExpression ve = new ValueSpecification(_sc, sv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(Parenthesis prnths) {
        //ValueExpression tree contains information about precedence
        //  this is why ValueExpression there is no ParenthesisValueExpression
        if(!isRecognized(prnths)){
            prnths.getExpression().accept(this);
        }
    }

    @Override
    public void visit(Addition adtn) {
        if(!isRecognized(adtn)){
            //normal call
            visitBinaryOperation(adtn);

            ValueExpression right = _exprStack.pop();
            ValueExpression left = _exprStack.pop();

            ValueExpression add = new plan_runner.expressions.Addition(left, right);
            _exprStack.push(add);
        }
    }

    @Override
    public void visit(Division dvsn) {
        if(!isRecognized(dvsn)){
             //normal call
            visitBinaryOperation(dvsn);

            ValueExpression right = _exprStack.pop();
            ValueExpression left = _exprStack.pop();

            ValueExpression add = new plan_runner.expressions.Division(left, right);
            _exprStack.push(add);
        }
    }

    @Override
    public void visit(Multiplication m) {
        if(!isRecognized(m)){
            //normal call
            visitBinaryOperation(m);

            ValueExpression right = _exprStack.pop();
            ValueExpression left = _exprStack.pop();

            ValueExpression add = new plan_runner.expressions.Multiplication(left, right);
            _exprStack.push(add);
        }
    }

    @Override
    public void visit(Subtraction s) {
        if(!isRecognized(s)){
            //normal call
            visitBinaryOperation(s);

            ValueExpression right = _exprStack.pop();
            ValueExpression left = _exprStack.pop();

            ValueExpression add = new plan_runner.expressions.Subtraction(left, right);
            _exprStack.push(add);
        }
    }

    /*
     * We have the similar code in Index Visitors.
     *   It has to be there
     *     in case that we decide not to do projections on complex expression as soon as possible
     *     (for example when we need to send both EXTRACT_YEAR(ORDERDATE) and ORDERDATE itself)
     */
    @Override
    public void visit(Function function) {
        if(!isRecognized(function)){
            //normal call
            //all aggregate functions (SUM, AVG, COUNT, MAX, MIN) should never appear here
            ExpressionList params = function.getParameters();
            int numParams = 0;
            if(params != null){
                params.accept(this);

                //in order to determine the size
                List<Expression> listParams = params.getExpressions();
                numParams = listParams.size();
            }
            List<ValueExpression> expressions = new ArrayList<ValueExpression>();
            for(int i=0; i<numParams; i++){
                expressions.add(_exprStack.pop());
            }
            Collections.reverse(expressions); // at the stack top is the lastly added VE

            String fnName = function.getName();
            if(fnName.equalsIgnoreCase("EXTRACT_YEAR")){
                if(numParams != 1){
                    throw new RuntimeException("EXTRACT_YEAR function has exactly one parameter!");
                }
                ValueExpression expr = expressions.get(0);
                ValueExpression ve = new IntegerYearFromDate(expr);
                _exprStack.push(ve);
            }
        }
    }

    @Override
    public void visit(ExpressionList el) {
        for (Iterator iter = el.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
        }
    }

    private void visitBinaryOperation(BinaryExpression be){
        be.getLeftExpression().accept(this);
        be.getRightExpression().accept(this);
    }

    /*
     * returns true if an expression was found in tupleSchema
     *   true means no need to call parent
     * It has side effects - putting on exprStack
     */
    private <T extends Expression> boolean isRecognized(T expr){
        //expr is changed in place, so that it does not contain synonims
        int position = _nt.indexOf(_inputTupleSchema, expr);
        if(position != ParserUtil.NOT_FOUND){
            //we found an expression already in the tuple schema
            TypeConversion tc = _nt.getType(_inputTupleSchema, expr);
            ValueExpression ve = new ColumnReference(tc, position, ParserUtil.getStringExpr(expr));
            pushToExprStack(ve);
            return true;
        }else{
            return false;
        }
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
    public void visit(AndExpression ae) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(OrExpression oe) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(Between btwn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(EqualsTo et) {
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