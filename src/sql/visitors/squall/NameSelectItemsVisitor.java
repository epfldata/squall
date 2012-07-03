package sql.visitors.squall;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import plan_runner.components.Component;
import plan_runner.conversion.StringConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueExpression;
import sql.optimizers.cost.NameTranslator;
import sql.util.ParserUtil;
import sql.util.TupleSchema;


public class NameSelectItemsVisitor extends IndexSelectItemsVisitor{
    private NameTranslator _nt;

    private TupleSchema _tupleSchema;
    
    private final static StringConversion _sc = new StringConversion();

    public NameSelectItemsVisitor(TupleSchema tupleSchema, Map map, Component affectedComponent){
        super(map);
        
        _tupleSchema = tupleSchema;
        _nt = new NameTranslator(affectedComponent.getName());
    }    
    
    @Override
    public void visit(Parenthesis prnths) {
        if(!isRecognized(prnths)){
            //normal call to parent
            super.visit(prnths);
        }
    }    

    @Override
    public void visit(Addition adtn) {
        if(!isRecognized(adtn)){
            //normal call to parent
            super.visit(adtn);
        }
    }

    @Override
    public void visit(Multiplication m) {
        if(!isRecognized(m)){
            //normal call to parent
            super.visit(m);
        }
    }

    @Override
    public void visit(Division dvsn) {
        if(!isRecognized(dvsn)){
            //normal call to parent
            super.visit(dvsn);
        }
    }

    @Override
    public void visit(Subtraction s) {
        if(!isRecognized(s)){
            //normal call to parent
            super.visit(s);
        }
    }

    @Override
    public void visit(Function function) {
        boolean recognized = isRecognized(function);
        if(!recognized){
            //try to extract SUM
            //parameters for COUNT are ignored, as explained in super
            String fnName = function.getName();
            if(fnName.equalsIgnoreCase("SUM")){
                recognized = isRecognized(function.getParameters());
                if(recognized){
                    ValueExpression expr = popFromExprStack();
                    createSum(expr, function.isDistinct());
                }
            }else if(fnName.equalsIgnoreCase("COUNT")){
                List<ValueExpression> expressions = new ArrayList<ValueExpression>();
                
                if(function.isDistinct()){
                    //putting something on stack only if isDistinct is set to true
                    recognized = isRecognized(function.getParameters());
                    //it might happen that we put on stack something we don't use
                    //  this is the case only when some exprs are recognized and the others not
                    if(recognized){
                    
                        //create a list of expressions
                        int numParams = 0;
                        ExpressionList params = function.getParameters();
                        if(params != null){
                            numParams = params.getExpressions().size();
                        }
                        
                        for(int i=0; i<numParams; i++){
                            expressions.add(popFromExprStack());
                        }
                    }
                }else{
                    recognized = true;
                }
                if(recognized){
                    //finally, create CountAgg out of expressions (empty if nonDistinct)
                    createCount(expressions, function.isDistinct());
                }
            }
        }
        if(!recognized){
            //normal call to parent
            super.visit(function);
        }
    }

    /*
     * returns true if an expression was found in tupleSchema
     *   true means no need to call parent, somthing added to stack
     * It has side effects - putting on exprStack
     */
    private <T extends Expression> boolean isRecognized(T expr){
        //expr is changed in place, so that it does not contain synonims
        int position = _nt.indexOf(_tupleSchema, expr);
        if(position != ParserUtil.NOT_FOUND){
            //we found an expression already in the tuple schema
            TypeConversion tc = _nt.getType(_tupleSchema, expr);
            ValueExpression ve = new ColumnReference(tc, position, ParserUtil.getStringExpr(expr));
            pushToExprStack(ve);
            return true;
        }else{
            return false;
        }
    }

    /*
     * Has to be separate because ExpressionList does not extend Expression
     */
    private boolean isRecognized(ExpressionList params) {
        if(params == null){
            return true;
        }
        
        List<Expression> exprs = params.getExpressions();
        if(exprs == null || exprs.isEmpty()){
            return true;
        }
        
        //if any of the children is not recognized, we have to go to super
        //  TODO(but fine): if some of exprs are recognized and some not, we will have some extra elements on stack
        for(Expression expr: exprs){
            if(!isRecognized(expr)){
                return false;
            }
        }
        
        //all exprs recognized
        return true;
    }

    /*
     * only getColumnIndex method invocation is different than in parent
     */
    @Override
    public void visit(Column column) {
        //extract type for the column
        //TypeConversion tc = ParserUtil.getColumnType(column, _tan, _schema);

        //TODO: Due to the fact that Project prepares columns for FinalAgg on last component
        //        and that for SUM or COUNT we are not going to this method (recognize is true),
        //        this method is invoked only for GroupByProjections as the top level method.
        //      That is, we can safely assume StringConversion method.
        //      Permanent fix is to create StringConversion over overallAggregation.
        TypeConversion tc = _sc;
        
        //extract the position (index) of the required column
        int position = _nt.getColumnIndex(_tupleSchema, column);

        ValueExpression ve = new ColumnReference(tc, position, ParserUtil.getStringExpr(column));
        pushToExprStack(ve);
    }

}