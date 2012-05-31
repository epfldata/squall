/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors.squall;

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
import java.util.List;
import java.util.Map;
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
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;
import operators.AggregateCountOperator;
import operators.AggregateOperator;
import operators.AggregateSumOperator;
import operators.DistinctOperator;
import optimizers.OptimizerTranslator;
import queryPlans.QueryPlan;
import schema.Schema;
import util.ParserUtil;
import util.TableAliasName;

/*
 * Generates Aggregation and
 */
public class SelectItemsVisitor implements SelectItemVisitor, ExpressionVisitor{
    private Schema _schema;
    private QueryPlan _queryPlan;
    private Component _affectedComponent;
    private TableAliasName _tan;
    private OptimizerTranslator _ot;
    private Map _map;

    private Stack<ValueExpression> _exprStack;
    private AggregateOperator _agg = null;

    //these two are of interest for the invoker
    private List<AggregateOperator> _aggOps = new ArrayList<AggregateOperator>();
    private List<ValueExpression> _groupByVEs = new ArrayList<ValueExpression>();

    public static final int AGG = 0;
    public static final int NON_AGG = 1;

    public SelectItemsVisitor(QueryPlan queryPlan, Schema schema, TableAliasName tan, OptimizerTranslator ot, Map map){
        _queryPlan = queryPlan;
        _schema = schema;
        _tan = tan;
        _ot = ot;
        _map = map;

        _affectedComponent = queryPlan.getLastComponent();
    }

    public List<AggregateOperator> getAggOps(){
        return _aggOps;
    }

    public List<ValueExpression> getGroupByVEs(){
        return _groupByVEs;
    }

    //SELECTITEMVISITOR DESIGN PATTERN
    @Override
    public void visit(AllColumns ac) {
        //i.e. SELECT * FROM R join S
        //we need not to do anything in this case
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
            List<Expression> listParams = params.getExpressions();
            numParams = listParams.size();
            for(Expression param: listParams){
                param.accept(this);
            }
        }
        List<ValueExpression> expressions = new ArrayList<ValueExpression>();
        for(int i=0; i<numParams; i++){
            expressions.add(_exprStack.pop());
        }

        String fnName = function.getName();
        if(fnName.equalsIgnoreCase("SUM")){
            //there must be only one parameter, if not SQL parser will raise an exception
            ValueExpression expr = expressions.get(0);
            NumericConversion numConv = (NumericConversion) expr.getType();
            _agg = new AggregateSumOperator(numConv, expr, _map);

            //DISTINCT and agg are stored on the same component.
            if(function.isDistinct()){
                DistinctOperator distinct = new DistinctOperator(_map, expressions);
                _agg.setDistinct(distinct);
            }
        }else if(fnName.equalsIgnoreCase("COUNT")){
            //COUNT(R.A) and COUNT(1) have the same semantics as COUNT(*), since we do not have NULLs in R.A
            _agg = new AggregateCountOperator(_map);

            //DISTINCT and agg are stored on the same component.
            if(function.isDistinct()){
                DistinctOperator distinct = new DistinctOperator(_map, expressions);
                _agg.setDistinct(distinct);
            }
        }else if (fnName.equalsIgnoreCase("EXTRACT_YEAR")) {
            if(numParams != 1){
                throw new RuntimeException("EXTRACT_YEAR function has exactly one parameter!");
            }
            ValueExpression expr = expressions.get(0);
            ValueExpression ve = new IntegerYearFromDate(expr);
            _exprStack.push(ve);
        }
    }

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
        String tableSchemaName = _tan.getSchemaName(ParserUtil.getComponentName(column.getTable()));
        String columnName = column.getColumnName();
        TypeConversion tc = _schema.getType(tableSchemaName, columnName);

        //extract the position (index) of the required column
        int position = _ot.getColumnIndex(column, _affectedComponent, _queryPlan, null);

        ValueExpression ve = new ColumnReference(tc, position);
        _exprStack.push(ve);
    }

    @Override
    public void visit(DoubleValue dv) {
        ValueExpression ve = new ValueSpecification(new DoubleConversion(), dv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(LongValue lv) {
        ValueExpression ve = new ValueSpecification(new LongConversion(), lv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(DateValue dv) {
        ValueExpression ve = new ValueSpecification(new DateConversion(), dv.getValue());
        _exprStack.push(ve);
    }

    @Override
    public void visit(StringValue sv) {
        ValueExpression ve = new ValueSpecification(new StringConversion(), sv.getValue());
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
