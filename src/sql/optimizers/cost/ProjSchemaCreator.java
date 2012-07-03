package sql.optimizers.cost;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import plan_runner.components.Component;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.ProjectOperator;
import sql.schema.ColumnNameType;
import sql.schema.Schema;
import sql.util.JoinTablesExprs;
import sql.util.ParserUtil;
import sql.util.TableAliasName;
import sql.util.TupleSchema;
import sql.visitors.jsql.MaxSubExpressionsVisitor;
import sql.visitors.jsql.SQLVisitor;
import sql.visitors.squall.NameProjectVisitor;

/*
 * This class takes expressions from GlobalProjExpr,
 *   add to them those from Hashes,
 *   and create an output schema based on which expressions are required down the topology.
 * Correctly recognized subexpressions as well.
 */
public class ProjSchemaCreator {
    private final ProjGlobalCollect _globalProject; // this is shared by all the ProjSchemaCreator objects
    private final TupleSchema _inputTupleSchema;

    private final NameTranslator _nt;
    private final TableAliasName _tan; //used for getting a list of all the tableCompNames
    private final Schema _schema;
    private final JoinTablesExprs _jte; //used for getting joinCondition
    private final Component _component;
    private final SQLVisitor _pq;
    private final NameProjectVisitor _npv;

    //output of this class
    private TupleSchema _outputTupleSchema;
    private List<ValueExpression> _veList;
    
    private static final IntegerConversion _ic = new IntegerConversion();

    public ProjSchemaCreator(ProjGlobalCollect globalProject, TupleSchema inputTupleSchema, Component component, 
            SQLVisitor pq, Schema schema){

        _globalProject = globalProject;
        _inputTupleSchema = inputTupleSchema;
        _tan = pq.getTan();
        _schema = schema;
        _jte = pq.getJte();
        _component = component;
        _pq = pq;

        _nt = new NameTranslator(component.getName());
        _npv = new NameProjectVisitor(_inputTupleSchema, _tan, _schema, component);
    }

    /*
     * can be invoked multiple times with no harm
     */
    public void create(){
        List<Expression> exprList = new ArrayList<Expression>();

        //these methods adds to exprList
        //each added expression is either present in inputTupleSchema, or can be built out of it
        processGlobalExprs(exprList);
        processGlobalOrs(exprList);
        if(!ParserUtil.isFinalJoin(_component, _pq)){
            //last component does not have hashes, because it's joined with noone
            processHashes(exprList);
        }

        //choose for which expressions we do projection, and create a schema out of that
        List<Expression> chosenExprs = chooseProjections(exprList);
        _outputTupleSchema = createSchema(chosenExprs);

        //convert JSQL to Squall expressions
        _npv.visit(chosenExprs);
        _veList = _npv.getExprs();

    }

    public TupleSchema getOutputSchema(){
        return _outputTupleSchema;
    }

    /*
     * will be used for a creation of a ProjectOperator
     */
    public ProjectOperator getProjectOperator(){
        return new ProjectOperator(_veList);
    }

    /*
     * For each expression from _globalProject (for now these are only from SELECT clause),
     *   add the appropriate subexpressions to _exprList
     */
    private void processGlobalExprs(List<Expression> exprList) {
        MaxSubExpressionsVisitor sev = new MaxSubExpressionsVisitor(_nt, _inputTupleSchema);
        sev.visit(_globalProject.getExprList());
        exprList.addAll(sev.getExprs());
    }

    /*
     * OrExpressions are from WHERE clause
     *   We need to project for it if they are not already executed
     * This could work without subexpressions, 
     *   because SelectOperator can work with ValueExpressions (and not only with ColumnReferences)
     */
    private void processGlobalOrs(List<Expression> exprList) {
        List<OrExpression> orList = _globalProject.getOrExprs();
        if(orList != null){
            for(OrExpression orExpr: _globalProject.getOrExprs()){
                MaxSubExpressionsVisitor sev = new MaxSubExpressionsVisitor(_nt, _inputTupleSchema);
                sev.visit(orExpr);
                if(!sev.isAllSubsMine(orExpr)){
                    //if all of them are available, SELECT operator is already done 
                    //  (either in this component because SELECT goes before PROJECT
                    //   or in some of ancestor components)
                
                    //we get all the subexpressions correlated to me
                    List<Expression> mineSubExprs = sev.getExprs();
                    exprList.addAll(mineSubExprs);
                }
            }
        }
    }

    /*
     * All the HashExpressions for joinining between ancestor of component and all other tables are collected
     */
    private void processHashes(List<Expression> exprList) {
        List<String> ancestorNames = ParserUtil.getSourceNameList(_component);

        //it has to be done like this, because queryPlan is not finished
        //  and does not contain all the tables yet
        List<String> allCompNames = _tan.getComponentNames();
        List<String> otherCompNames = ParserUtil.getDifference(allCompNames, ancestorNames);

        //now we find joinCondition between ancestorNames and otherCompNames
        //joinExprs is a list of EqualsTo
        List<Expression> joinExprs = _jte.getExpressions(ancestorNames, otherCompNames);

        MaxSubExpressionsVisitor sev = new MaxSubExpressionsVisitor(_nt, _inputTupleSchema);
        sev.visit(joinExprs);
        //we get all the subexpressions correlated to me
        List<Expression> mineSubExprs = sev.getExprs();
        exprList.addAll(mineSubExprs);
        
    }

    /*
     * Expressions from exprList are all appeapring somewhere in the query plan
     *   This method never can raise an exception, it can only cause suboptimality
     * There is no mandatory projections 
     *   because SelectOperator, AggregateOperator and Hashes are able to deal with ValueExpressions
     *   (and not only with ColumnReferences).
     * For example, if I have inputTupleSchema "R.A, R.A + R.B" in R and exprList "R.A, R.A + R.B"
     *     and decide to go with outputTupleSchema "R.A, R.B", there is no "R.B" in inputTupleSchema
     *     However, this is not possible, because parent will never send something like this "R.A, R.A + R.B".
     */
    private List<Expression> chooseProjections(List<Expression> exprList) {
        //colect all the columnNames from JSQL Column Expressions
        List<String> aloneColumnNames = getAloneColumnNames(exprList);

        List<Expression> resultExpr = new ArrayList<Expression>();
        for(Expression expr: exprList){
            if(expr instanceof Column){
                resultExpr.add(expr); // all the Column Expressions should be added
            }else{
                //all the columns used in expression expr
                List<Column> exprColumns = ParserUtil.getJSQLColumns(expr);
                boolean existsAlone = false; // does at least one column from expr already appears in aloneColumnNames?
                for(Column column: exprColumns){
                    String columnStr = ParserUtil.getStringExpr(column);
                    if(aloneColumnNames != null && aloneColumnNames.contains(columnStr)){
                        existsAlone = true;
                        break;
                    }
                }
                if(existsAlone){
                    //all the columns should be added to result
                    resultExpr.addAll(exprColumns);
                }else{
                    //add whole expr
                    resultExpr.add(expr);
                }
            }
        }

        //now take care of duplicates
        resultExpr = eliminateDuplicates(resultExpr);

        return resultExpr;
    }

    /*
     * Return all the columns which appears alone in its String form
     *   e.g. for R(A), R(A) + 5, R(B), R(C) + 2 this methods return R(A), R(B)
     */
    private List<String> getAloneColumnNames(List<Expression> exprList) {
        List<String> result = new ArrayList<String>();
        for(Expression expr: exprList){
            if(expr instanceof Column){
                Column column = (Column) expr;
                result.add(ParserUtil.getStringExpr(column));
            }
        }
        return result;
    }

    /*
     * We have to convert it to String, because that's the way we implemented equals operator
     *   (we don't want to change JSQL classes to add equals operator)
     */
    private List<Expression> eliminateDuplicates(List<Expression> exprList) {
        List<Expression> result = new ArrayList<Expression>();
        List<String> exprStrList = new ArrayList<String>();
        for(Expression expr: exprList){
            String exprStr = ParserUtil.getStringExpr(expr);
            if(!exprStrList.contains(exprStr)){
                //if it is not already there, add it
                result.add(expr);
            }
            //anyway we update a list of strings
            exprStrList.add(exprStr);
        }
        return result;
    }

    /*
     * Create new schema, but preserve all the synonims from _inputTupleSchema
     */
    private TupleSchema createSchema(List<Expression> choosenExprs) {
        List<ColumnNameType> cnts = new ArrayList<ColumnNameType>();

        for(Expression expr: choosenExprs){
            //first to determine the type, we use the first column for that
            
            TypeConversion tc = getTC(expr);

            //attach the TypeConversion
            String exprStr = ParserUtil.getStringExpr(expr);
            ColumnNameType cnt = new ColumnNameType(exprStr, tc);
            cnts.add(cnt);
        }

        //copying all the synonims from inputTupleSchema
        TupleSchema result = new TupleSchema(cnts);
        Map<String, Column> inputSynonims = _inputTupleSchema.getSynonims();
        if(inputSynonims != null){
            result.setSynonims(inputSynonims);
        }
        
        return result;
    }

    /*
     * Have to distinguish special cases from normal ones
     */
    private TypeConversion getTC(Expression expr) {
        if(expr instanceof Function){
            Function fun = (Function) expr;
            if(fun.getName().equalsIgnoreCase("EXTRACT_YEAR")){
                return _ic;
            }
        }
        
        //non special cases
        List<Column> columns = ParserUtil.getJSQLColumns(expr);
        Column column = columns.get(0);
        return ParserUtil.getColumnType(column, _tan, _schema);
    }

}