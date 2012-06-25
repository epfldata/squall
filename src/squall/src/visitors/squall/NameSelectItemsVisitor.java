package visitors.squall;

import conversion.TypeConversion;
import expressions.ColumnReference;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import optimizers.cost.NameTranslator;
import schema.ColumnNameType;
import schema.Schema;
import util.ParserUtil;
import util.TableAliasName;


public class NameSelectItemsVisitor extends IndexSelectItemsVisitor{
    private Schema _schema;
    private TableAliasName _tan;
    private NameTranslator _nt = new NameTranslator();

    private List<ColumnNameType> _tupleSchema;

    public NameSelectItemsVisitor(Schema schema, TableAliasName tan, List<ColumnNameType> tupleSchema, Map map){
        super(map);
        
        _schema = schema;
        _tan = tan;
        _tupleSchema = tupleSchema;
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
                recognized = isRecognized(function.getParameters());
                if(recognized){
                    
                    //create a list of expressions
                    int numParams = 0;
                    ExpressionList params = function.getParameters();
                    if(params != null){
                        numParams = params.getExpressions().size();
                    }
                    List<ValueExpression> expressions = new ArrayList<ValueExpression>();
                    for(int i=0; i<numParams; i++){
                        expressions.add(popFromExprStack());
                    }

                    //finally, create CountAgg out of it
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
        String strExpr = ParserUtil.getStringExpr(expr);

        int position = _nt.indexOf(_tupleSchema, strExpr);
        if(position != -1){
            //we found an expression already in the tuple schema
            TypeConversion tc = _nt.getType(_tupleSchema, strExpr);
            ValueExpression ve = new ColumnReference(tc, position);
            pushToExprStack(ve);
            return true;
        }else{
            return false;
        }
    }

    /*
     * Has to be separate because ExpressionList does not extend Expression
     * Exactly the same code as above
     */
    private boolean isRecognized(ExpressionList params) {
        if(params == null){
            return false;
        }

        String strExpr = ParserUtil.getStringExpr(params);

        int position = _nt.indexOf(_tupleSchema, strExpr);
        if(position != -1){
            //we found an expression already in the tuple schema
            TypeConversion tc = _nt.getType(_tupleSchema, strExpr);
            ValueExpression ve = new ColumnReference(tc, position);
            pushToExprStack(ve);
            return true;
        }else{
            return false;
        }
    }

    /*
     * only getColumnIndex method invocation is different than in parent
     */
    @Override
    public void visit(Column column) {
        //extract type for the column
        TypeConversion tc = ParserUtil.getColumnType(column, _tan, _schema);

        //extract the position (index) of the required column
        int position = _nt.getColumnIndex(column, _tupleSchema);

        ValueExpression ve = new ColumnReference(tc, position);
        pushToExprStack(ve);
    }
}