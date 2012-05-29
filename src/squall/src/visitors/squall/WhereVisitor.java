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
import net.sf.jsqlparser.statement.select.SubSelect;
import operators.SelectionOperator;
import optimizers.OptimizerTranslator;
import predicates.AndPredicate;
import predicates.ComparisonPredicate;
import predicates.LikePredicate;
import predicates.OrPredicate;
import predicates.Predicate;
import queryPlans.QueryPlan;
import schema.Schema;
import util.ParserUtil;
import util.TableAliasName;


public class WhereVisitor implements ExpressionVisitor {
    private Stack<ValueExpression> _exprStack = new Stack<ValueExpression>();
    private Stack<Predicate> _predStack = new Stack<Predicate>();

    private Schema _schema;
    private QueryPlan _queryPlan;

    //necessary for getColumnIndex in visitColumn method
    private Component _affectedComponent;
    private TableAliasName _tan;
    private OptimizerTranslator _ot;

    public WhereVisitor(QueryPlan queryPlan, Component affectedComponent, Schema schema, TableAliasName tan, OptimizerTranslator ot){
        _queryPlan = queryPlan;
        _affectedComponent = affectedComponent;
        _schema = schema;
        _tan = tan;
        _ot = ot;

        _affectedComponent = affectedComponent;
    }

    public Predicate getPredicate(){
        if(_predStack.size()!=1){
            throw new RuntimeException("After WhereVisitor is done, it should contain one predicate exactly!");
        }
        return _predStack.peek();
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
    public void visit(Column column) {
        //extract type for the column
        String tableSchemaName = _tan.getSchemaName(ParserUtil.getComponentName(column.getTable()));
        String columnName = column.getColumnName();
        TypeConversion tc = _schema.getType(tableSchemaName, columnName);

        //extract the position (index) of the required column
        int position = _ot.getColumnIndex(column, _affectedComponent, _queryPlan, _schema, _tan);

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
