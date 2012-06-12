/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package visitors.squall;

import conversion.TypeConversion;
import expressions.ColumnReference;
import expressions.ValueExpression;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;
import optimizers.Translator;
import optimizers.cost.NameTranslator;
import schema.ColumnNameType;
import schema.Schema;
import util.ParserUtil;
import util.TableAliasName;
import visitors.jsql.PrintVisitor;


public class NameWhereVisitor extends IndexWhereVisitor{
    private Schema _schema;
    private TableAliasName _tan;
    private Translator _ot;
    
    private List<ColumnNameType> _tupleSchema;
    private PrintVisitor _printer = new PrintVisitor();

    public NameWhereVisitor(Schema schema, TableAliasName tan, List<ColumnNameType> tupleSchema){
        _schema = schema;
        _tan = tan;
        _tupleSchema = tupleSchema;
        
        _ot = new NameTranslator();
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
        if(!isRecognized(function)){
            //normal call to parent
            super.visit(function);
        }
    }


    /*
     * returns true if an expression was found in tupleSchema
     *   true means no need to call parent
     * It has side effects - putting on exprStack
     */
    private <T extends Expression> boolean isRecognized(T expr){
        expr.accept(_printer);
        String strExpr = _printer.getString();

        int position = _ot.indexOf(_tupleSchema, strExpr);
        if(position != -1){
            //we found an expression already in the tuple schema
            TypeConversion tc = _ot.getType(_tupleSchema, strExpr);
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
        String tableSchemaName = _tan.getSchemaName(ParserUtil.getComponentName(column));
        String columnName = column.getColumnName();
        TypeConversion tc = _schema.getType(tableSchemaName, columnName);

        //extract the position (index) of the required column
        int position = _ot.getColumnIndex(column, _tupleSchema);

        ValueExpression ve = new ColumnReference(tc, position);
        pushToExprStack(ve);
    }
}