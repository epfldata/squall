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
import sql.optimizers.index.IndexTranslator;
import sql.schema.Schema;
import sql.util.NotFromMyBranchException;
import sql.util.ParserUtil;
import sql.util.TableAliasName;

/*
 * Returns a list of hashExpressions for a given component in a given query plan.
 * If we do not have complexCondition, 
 *   outside this class hashIndexes are extracted from hashExpressions.
 *
 * For example, on join condition R.B = S.B, from component R,
 *   we need to find the index of B. This is not so easy if
 *    a) R is not a DataSourceComponent)
 *    b) many operators down the path
 */
public class IndexJoinHashVisitor implements ExpressionVisitor, ItemsListVisitor {
    //these are only used within visit(Column) method
    private Schema _schema;
    private Component _affectedComponent;
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

    private Stack<ValueExpression> _exprStack = new Stack<ValueExpression>();

    private List<ValueExpression> _hashExpressions = new ArrayList<ValueExpression>();

    /*
     * affectedComponent is one of the parents of the join component
     */
    public IndexJoinHashVisitor(Schema schema, Component affectedComponent, TableAliasName tan){
        _schema = schema;
        _affectedComponent = affectedComponent;
        _tan = tan;

        _it = new IndexTranslator(_schema, _tan);
    }

    protected IndexJoinHashVisitor(){}

    public List<ValueExpression> getExpressions(){
        return _hashExpressions;
    }

    protected void pushToExprStack(ValueExpression ve){
        _exprStack.push(ve);
    }
   
    @Override
    public void visit(AndExpression ae) {
        visitBinaryOperation(ae);
    }

    @Override
    public void visit(EqualsTo et) {
        visitSideEquals(et.getLeftExpression());
        visitSideEquals(et.getRightExpression());
    }

    private void visitSideEquals(Expression ex){
        try{
            _exprStack = new Stack<ValueExpression>();
            ex.accept(this);
            ValueExpression ve = _exprStack.pop();
            _hashExpressions.add(ve);
        }catch(NotFromMyBranchException exc){
            //expression would not be added in the list
        }
    }

    /*
     * Each of these operations create a Squall type, that's why so much similar code
     */
    @Override
    public void visit(Addition adtn) {
        visitBinaryOperation(adtn);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ValueExpression add = new plan_runner.expressions.Addition(left, right);
        _exprStack.push(add);
    }

    @Override
    public void visit(Multiplication m) {
        visitBinaryOperation(m);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ValueExpression mult = new plan_runner.expressions.Multiplication(left, right);
        _exprStack.push(mult);
    }

    @Override
    public void visit(Division dvsn) {
        visitBinaryOperation(dvsn);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ValueExpression division = new plan_runner.expressions.Division(left, right);
        _exprStack.push(division);
    }

    @Override
    public void visit(Subtraction s) {
        visitBinaryOperation(s);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        ValueExpression sub = new plan_runner.expressions.Subtraction(left, right);
        _exprStack.push(sub);
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
        int numParams = 0;
        if(params != null){
            visit(params);

            //only for size
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
        String tableCompName = ParserUtil.getComponentName(column);
        List<String> ancestorNames = ParserUtil.getSourceNameList(_affectedComponent);

        if(ancestorNames.contains(tableCompName)){
            //extract type for the column
            TypeConversion tc = _schema.getType(ParserUtil.getFullSchemaColumnName(column, _tan));

            //extract the position (index) of the required column
            int position = _it.getColumnIndex(column, _affectedComponent);

            ValueExpression ve = new ColumnReference(tc, position);
            _exprStack.push(ve);
        }else{
            throw new NotFromMyBranchException();
        }
    }

    //all of ValueSpecifications (constants) guarantee we have some expressions in join conditions
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