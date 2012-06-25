package visitors.squall;

import components.Component;
import conversion.*;
import expressions.ColumnReference;
import expressions.IntegerYearFromDate;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import operators.SelectOperator;
import optimizers.IndexTranslator;
import predicates.*;
import queryPlans.QueryPlan;
import schema.Schema;
import util.ParserUtil;
import util.TableAliasName;

/*
 * Translates JSQL expressions to a SelectionOperator of a component.
 *   JSQL expressions *must* refer only to the component.
 */
public class IndexWhereVisitor implements ExpressionVisitor, ItemsListVisitor {
    private Stack<ValueExpression> _exprStack = new Stack<ValueExpression>();
    private Stack<Predicate> _predStack = new Stack<Predicate>();

    //all of these are necessary only for getColumnIndex in visitColumn method
    private Component _affectedComponent;
    private QueryPlan _queryPlan;
    private Schema _schema;
    private TableAliasName _tan;
    private IndexTranslator _it;

    //this will not break any contracts,
    //  even with new DateConversion() on all the places,
    //  we will have a single object per (possibly) multiple spout/bolt threads.
    //generating plans is done from a single thread, static additionally saves space
    private static LongConversion _lc = new LongConversion();
    private static DoubleConversion _dblConv = new DoubleConversion();
    private static DateConversion _dateConv = new DateConversion();
    private static StringConversion _sc = new StringConversion();

    public IndexWhereVisitor(QueryPlan queryPlan, Component affectedComponent, Schema schema, TableAliasName tan){
        _queryPlan = queryPlan;
        _affectedComponent = affectedComponent;
        _schema = schema;
        _tan = tan;
        _it = new IndexTranslator(_schema, _tan);

        _affectedComponent = affectedComponent;
    }

    /*
     * Used from NameWhereVisitor - no parameters need to be set
     */
    protected IndexWhereVisitor(){}

    public SelectOperator getSelectOperator(){
        if(_predStack.size() != 1){
            throw new RuntimeException("After WhereVisitor is done, it should contain one predicate exactly!");
        }
        return new SelectOperator(_predStack.peek());
    }

    protected void pushToExprStack(ValueExpression ve){
        _exprStack.push(ve);
    }

    @Override
    public void visit(AndExpression ae) {
        visitBinaryOperation(ae);
        
        Predicate right = _predStack.pop();
        Predicate left = _predStack.pop();
        
        Predicate and = new AndPredicate(left, right);
        _predStack.push(and);
    }

    @Override
    public void visit(OrExpression oe) {
        visitBinaryOperation(oe);
        
        Predicate right = _predStack.pop();
        Predicate left = _predStack.pop();
        
        Predicate or = new OrPredicate(left, right);
        _predStack.push(or);
    }

    /*
     * Each of these operations create a Squall type, that's why so much similar code
     */
    @Override
    public void visit(Addition adtn) {
        visitBinaryOperation(adtn);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion nc = (NumericConversion) left.getType();

        ValueExpression add = new expressions.Addition(nc, left, right);
        _exprStack.push(add);
    }

    @Override
    public void visit(Multiplication m) {
        visitBinaryOperation(m);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion nc = (NumericConversion) left.getType();

        ValueExpression mult = new expressions.Multiplication(nc, left, right);
        _exprStack.push(mult);
    }

    @Override
    public void visit(Division dvsn) {
        visitBinaryOperation(dvsn);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion nc = (NumericConversion) left.getType();

        ValueExpression division = new expressions.Division(nc, left, right);
        _exprStack.push(division);
    }    

    @Override
    public void visit(Subtraction s) {
        visitBinaryOperation(s);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion nc = (NumericConversion) left.getType();

        ValueExpression sub = new expressions.Subtraction(nc, left, right);
        _exprStack.push(sub);
    }

    @Override
    public void visit(EqualsTo et) {
        visitBinaryOperation(et);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ComparisonPredicate cp = new ComparisonPredicate(ComparisonPredicate.EQUAL_OP, left, right);
        _predStack.push(cp);
    }

    @Override
    public void visit(LikeExpression le) {
        visitBinaryOperation(le);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        LikePredicate lp = new LikePredicate(left, right);
        _predStack.push(lp);
    }

    @Override
    public void visit(GreaterThan gt) {
        visitBinaryOperation(gt);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ComparisonPredicate cp = new ComparisonPredicate(ComparisonPredicate.GREATER_OP, left, right);
        _predStack.push(cp);
    }

    @Override
    public void visit(GreaterThanEquals gte) {
        visitBinaryOperation(gte);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ComparisonPredicate cp = new ComparisonPredicate(ComparisonPredicate.NONLESS_OP, left, right);
        _predStack.push(cp);
    }


    @Override
    public void visit(MinorThan mt) {
        visitBinaryOperation(mt);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ComparisonPredicate cp = new ComparisonPredicate(ComparisonPredicate.LESS_OP, left, right);
        _predStack.push(cp);
    }

    @Override
    public void visit(MinorThanEquals mte) {
        visitBinaryOperation(mte);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ComparisonPredicate cp = new ComparisonPredicate(ComparisonPredicate.NONGREATER_OP, left, right);
        _predStack.push(cp);
    }

    @Override
    public void visit(NotEqualsTo net) {
        visitBinaryOperation(net);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ComparisonPredicate cp = new ComparisonPredicate(ComparisonPredicate.NONEQUAL_OP, left, right);
        _predStack.push(cp);
    }

    private void visitBinaryOperation(BinaryExpression be){
        be.getLeftExpression().accept(this);
        be.getRightExpression().accept(this);
    }

    @Override
    public void visit(Function function) {
        //all aggregate functions (SUM, AVG, COUNT, MAX, MIN) have only one parameter (Expression)
        //although COUNT(*) has no parameters
        //EXTRACT_YEAR has one parameter
        //if you change this method, NameProjectVisitor.visit(Function) has to be changed as well
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

    @Override
    public void visit(ExpressionList el) {
        for (Iterator iter = el.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
        }
    }

    @Override
    public void visit(Column column) {
        //extract type for the column
        TypeConversion tc = ParserUtil.getColumnType(column, _tan, _schema);

        //extract the position (index) of the required column
        int position = _it.getColumnIndex(column, _affectedComponent, _queryPlan);

        ValueExpression ve = new ColumnReference(tc, position);
        _exprStack.push(ve);
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
