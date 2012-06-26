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
import net.sf.jsqlparser.statement.select.*;
import operators.AggregateCountOperator;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.DistinctOperator;
import optimizers.IndexTranslator;
import queryPlans.QueryPlan;
import schema.Schema;
import util.ParserUtil;
import util.TableAliasName;

/*
 * Generates Aggregations and its groupBy projections.
 *   If there is no aggregation, these groupBy projections becomes simple projections
 */
public class IndexSelectItemsVisitor implements SelectItemVisitor, ExpressionVisitor, ItemsListVisitor{
    //all of these needed only for visit(Column) method
    private Schema _schema;
    private QueryPlan _queryPlan;
    private Component _affectedComponent;
    private TableAliasName _tan;
    private IndexTranslator _it;

    //needed only for visit(Function) method
    private Map _map;

    private Stack<ValueExpression> _exprStack;
    private AggregateOperator _agg = null;

    //these two are of interest for the invoker
    private List<AggregateOperator> _aggOps = new ArrayList<AggregateOperator>();
    private List<ValueExpression> _groupByVEs = new ArrayList<ValueExpression>();

    public static final int AGG = 0;
    public static final int NON_AGG = 1;

    //this will not break any contracts,
    //  even with new DateConversion() on all the places,
    //  we will have a single object per (possibly) multiple spout/bolt threads.
    //generating plans is done from a single thread, static additionally saves space
    private static LongConversion _lc = new LongConversion();
    private static DoubleConversion _dblConv = new DoubleConversion();
    private static DateConversion _dateConv = new DateConversion();
    private static StringConversion _sc = new StringConversion();

    public IndexSelectItemsVisitor(QueryPlan queryPlan, Schema schema, TableAliasName tan, Map map){
        _queryPlan = queryPlan;
        _schema = schema;
        _tan = tan;
        _map = map;
        _affectedComponent = queryPlan.getLastComponent();

        _it = new IndexTranslator(_schema, _tan);
    }
    
    protected IndexSelectItemsVisitor(Map map){
        _map = map;
    }

    public List<AggregateOperator> getAggOps(){
        return _aggOps;
    }

    public List<ValueExpression> getGroupByVEs(){
        return _groupByVEs;
    }

    protected void pushToExprStack(ValueExpression ve){
        _exprStack.push(ve);
    }

    protected ValueExpression popFromExprStack(){
        return _exprStack.pop();
    }

    //SELECTITEMVISITOR DESIGN PATTERN
    @Override
    public void visit(AllColumns ac) {
        //i.e. SELECT * FROM R join S
        //we need not to do anything in this case for RuleOptimizer
        //TODO: support it for Cost-Optimizer (Each wanted column has to explicitly specified)
    }

    @Override
    public void visit(AllTableColumns atc) {
        //i.e. SELECT R.* FROM R join S
        //this is very rare, so it's not supported
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(SelectExpressionItem sei) {
        _exprStack = new Stack<ValueExpression>();
        _agg = null;
        sei.getExpression().accept(this);
        doneSingleItem();
    }

    private void doneSingleItem(){
        if(_agg == null){
            _groupByVEs.add(_exprStack.peek());
        }else{
            _aggOps.add(_agg);
        }
    }

    //my VISITOR methods
    @Override
    public void visit(Function function) {
        //all aggregate functions (SUM, AVG, COUNT, MAX, MIN) have only one parameter (Expression)
        //although COUNT(*) has no parameters
        //EXTRACT_YEAR has one parameter
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
        if(fnName.equalsIgnoreCase("SUM")){
            //there must be only one parameter, if not SQL parser will raise an exception
            ValueExpression expr = expressions.get(0);
            createSum(expr, function.isDistinct());
        }else if(fnName.equalsIgnoreCase("COUNT")){
            createCount(expressions, function.isDistinct());
        }else if (fnName.equalsIgnoreCase("EXTRACT_YEAR")) {
            if(numParams != 1){
                throw new RuntimeException("EXTRACT_YEAR function has exactly one parameter!");
            }
            ValueExpression expr = expressions.get(0);
            ValueExpression ve = new IntegerYearFromDate(expr);
            _exprStack.push(ve);
        }
    }

    protected void createSum(ValueExpression ve, boolean isDistinct){
        NumericConversion numConv = (NumericConversion) ve.getType();
        _agg = new AggregateSumOperator(numConv, ve, _map);

        //DISTINCT and agg are stored on the same component.
        if(isDistinct){
            DistinctOperator distinct = new DistinctOperator(_map, ve);
            _agg.setDistinct(distinct);
        }
    }

    protected void createCount(List<ValueExpression> veList, boolean isDistinct){
        //COUNT(R.A) and COUNT(1) have the same semantics as COUNT(*), since we do not have NULLs in R.A
        _agg = new AggregateCountOperator(_map);

        //DISTINCT and agg are stored on the same component.
        if(isDistinct){
            DistinctOperator distinct = new DistinctOperator(_map, veList);
            _agg.setDistinct(distinct);
        }
    }

    @Override
    public void visit(ExpressionList el) {
        for (Iterator iter = el.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
        }
    }


    /*
     * Each of these operations create a Squall type, that's why so much similar code
     */
    @Override
    public void visit(Addition adtn) {
        visitBinaryExpression(adtn);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion numConv = (NumericConversion) left.getType();
        //TODO: check whether they are both of the same type

        ValueExpression ve = new expressions.Addition(numConv, left, right);
        _exprStack.push(ve);
    }

    @Override
    public void visit(Multiplication m) {
        visitBinaryExpression(m);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion numConv = (NumericConversion) left.getType();
        //TODO: check whether they are both of the same type

        ValueExpression ve = new expressions.Multiplication(numConv, left, right);
        _exprStack.push(ve);
    }

    @Override
    public void visit(Division dvsn) {
        visitBinaryExpression(dvsn);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion numConv = (NumericConversion) left.getType();
        //TODO: check whether they are both of the same type

        ValueExpression ve = new expressions.Division(numConv, left, right);
        _exprStack.push(ve);
    }

    @Override
    public void visit(Subtraction s) {
        visitBinaryExpression(s);

        ValueExpression right = _exprStack.pop();
        ValueExpression left = _exprStack.pop();

        NumericConversion numConv = (NumericConversion) left.getType();
        //TODO: check whether they are both of the same type

        ValueExpression ve = new expressions.Subtraction(numConv, left, right);
        _exprStack.push(ve);

    }

    public void visitBinaryExpression(BinaryExpression binaryExpression) {
        binaryExpression.getLeftExpression().accept(this);
	binaryExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(Parenthesis prnths) {
        prnths.getExpression().accept(this);
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


    //EXPRESSIONVISITOR DESIGN PATTERN
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
