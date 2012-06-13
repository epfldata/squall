/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors.squall;

import util.NotFromMyBranchException;
import components.Component;
import conversion.DateConversion;
import conversion.DoubleConversion;
import conversion.LongConversion;
import conversion.NumericConversion;
import conversion.StringConversion;
import conversion.TypeConversion;
import expressions.ColumnReference;
import expressions.IntegerYearFromDate;
import expressions.ValueExpression;
import expressions.ValueSpecification;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import optimizers.IndexTranslator;
import optimizers.Translator;
import queryPlans.QueryPlan;
import schema.Schema;
import util.HierarchyExtractor;
import util.ParserUtil;
import util.TableAliasName;

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
    private QueryPlan _queryPlan;
    private Component _affectedComponent;
    private TableAliasName _tan;

    private Translator _ot;

    //this will not break any contracts,
    //  even with new DateConversion() on all the places,
    //  we will have a single object per (possibly) multiple spout/bolt threads.
    //generating plans is done from a single thread, static additionally saves space
    private static LongConversion _lc = new LongConversion();
    private static DoubleConversion _dblConv = new DoubleConversion();
    private static DateConversion _dateConv = new DateConversion();
    private static StringConversion _sc = new StringConversion();

    //complex condition means that we will use HashExpressions, not HashIndexes.
    //   i.e. R.A=S.A+1 is complexCondition, whereas R.A=S.A and R.B=S.B is not.
    //if complex condition appeared at least once,
    //   we decide to use HashExpressions all over the place,
    //   because of unspecified order of HashIndexes and HashExpression,
    //   when they are interleaved.

    private boolean _complexCondition = false;

    private Stack<ValueExpression> _exprStack = new Stack<ValueExpression>();

    private List<ValueExpression> _hashExpressions = new ArrayList<ValueExpression>();

    /*
     * affectedComponent is one of the parents of the join component
     */
    public IndexJoinHashVisitor(Schema schema, QueryPlan queryPlan, Component affectedComponent, TableAliasName tan){
        _schema = schema;
        _queryPlan = queryPlan;
        _affectedComponent = affectedComponent;
        _tan = tan;

        _ot = new IndexTranslator(_schema, _tan);
    }

    protected IndexJoinHashVisitor(){}

    public List<ValueExpression> getExpressions(){
        return _hashExpressions;
    }

    public boolean isComplexCondition(){
        return _complexCondition;
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
        _complexCondition = true;
        visitBinaryOperation(adtn);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion nc = (NumericConversion) left.getType();

        ValueExpression add = new expressions.Addition(nc, left, right);
        _exprStack.push(add);
    }

    @Override
    public void visit(Multiplication m) {
        _complexCondition = true;
        visitBinaryOperation(m);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion nc = (NumericConversion) left.getType();

        ValueExpression mult = new expressions.Multiplication(nc, left, right);
        _exprStack.push(mult);
    }

    @Override
    public void visit(Division dvsn) {
        _complexCondition = true;
        visitBinaryOperation(dvsn);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion nc = (NumericConversion) left.getType();

        ValueExpression division = new expressions.Division(nc, left, right);
        _exprStack.push(division);
    }

    @Override
    public void visit(Subtraction s) {
        _complexCondition = true;
        visitBinaryOperation(s);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion nc = (NumericConversion) left.getType();

        ValueExpression sub = new expressions.Subtraction(nc, left, right);
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
        _complexCondition = true;
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
        String tableSchemaName = _tan.getSchemaName(tableCompName);
        List<String> ancestorNames = HierarchyExtractor.getAncestorNames(_affectedComponent);

        if(ancestorNames.contains(tableCompName)){
            //extract type for the column
            String columnName = column.getColumnName();
            TypeConversion tc = _schema.getType(tableSchemaName, columnName);

            //extract the position (index) of the required column
            int position = _ot.getColumnIndex(column, _affectedComponent, _queryPlan);

            ValueExpression ve = new ColumnReference(tc, position);
            _exprStack.push(ve);
        }else{
            throw new NotFromMyBranchException();
        }
    }

    //all of ValueSpecifications (constants) guarantee we have some expressions in join conditions
    @Override
    public void visit(DoubleValue dv) {
        _complexCondition = true;
        ValueExpression ve = new ValueSpecification(_dblConv, dv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(LongValue lv) {
        _complexCondition = true;
        ValueExpression ve = new ValueSpecification(_lc, lv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(DateValue dv) {
        _complexCondition = true;
        ValueExpression ve = new ValueSpecification(_dateConv, dv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(StringValue sv) {
        _complexCondition = true;
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