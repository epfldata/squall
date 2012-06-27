package optimizers.cost;

import components.Component;
import components.DataSourceComponent;
import conversion.TypeConversion;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import operators.ProjectOperator;
import schema.ColumnNameType;
import schema.Schema;
import util.JoinTablesExprs;
import util.ParserUtil;
import util.TableAliasName;
import visitors.jsql.MaxSubExpressionsVisitor;
import visitors.jsql.SQLVisitor;
import visitors.squall.NameProjectVisitor;

/*
 * This class takes expressions from GlobalProjExpr,
 *   add to them those from Hashes,
 *   and create an output schema based on which expressions are required down the topology.
 * DOES NOT work with subexpressions (if we have (R.A + 5) * 3 never R.A + 5 will be recognized as subexpression).
 */
public class ProjSchemaCreator {
    private final ProjGlobalCollect _globalProject; // this is shared by all the ProjSchemaCreator objects
    private final List<ColumnNameType> _inputTupleSchema;

    private final NameTranslator _nt = new NameTranslator();
    private final TableAliasName _tan; //used for getting a list of all the tableCompNames
    private final Schema _schema;
    private final JoinTablesExprs _jte; //used for getting joinCondition
    private final Component _component;
    private final NameProjectVisitor _npv;

    //output of this class
    private List<ColumnNameType> _outputTupleSchema;
    private List<ValueExpression> _veList;

    public ProjSchemaCreator(ProjGlobalCollect globalProject, List<ColumnNameType> inputTupleSchema, Component component, 
            SQLVisitor pq, Schema schema){

        _globalProject = globalProject;
        _inputTupleSchema = inputTupleSchema;
        _tan = pq.getTan();
        _schema = schema;
        _jte = pq.getJte();
        _component = component;

        _npv = new NameProjectVisitor(_inputTupleSchema, _tan, _schema);
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
        processHashes(exprList);

        //choose for which expressions we do projection, and create a schema out of that
        List<Expression> chosenExprs = chooseProjections(exprList);
        _outputTupleSchema = createSchema(chosenExprs);

        //convert JSQL to Squall expressions
        _npv.visit(chosenExprs);
        _veList = _npv.getExprs();

    }

    public List<ColumnNameType> getOutputSchema(){
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
     *   It means that we might need to project it for later use
     * If we didn't send subexpressions then the following would not work:
     *   In R_S component (R.A + 4 = 10) or (T.A = 4) we would send R.A 
     *   and R.A + 4 could not be evaluated in SELECT operator in R_S_T
     */
    private void processGlobalOrs(List<Expression> exprList) {
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

    /*
     * All the HashExpressions for joinining between ancestor of component and all other tables are collected
     */
    private void processHashes(List<Expression> exprList) {
        List<DataSourceComponent> ancestors = _component.getAncestorDataSources();
        List<String> ancestorNames = ParserUtil.getSourceNameList(ancestors);

        //it has to be done like this, because queryPlan is not finished
        //  and does not contain all the tables yet
        List<String> allCompNames = _tan.getComponentNames();
        List<String> otherCompNames = ParserUtil.getDifference(allCompNames, ancestorNames);

        //now we find joinCondition between ancestorNames and otherCompNames
        //joinExprs is a list of EqualsTo
        List<Expression> joinExprs = _jte.getExpressions(ancestorNames, otherCompNames);

        //TODO(but fine): for now we only collect columns, we anyway don't have cardinality estimation for complex HashExpressions
        //  In R_S component we might have a joinCondition R.A + 4 = T.A, "R.A" is anyway in inputTupleSchema of R_S
        //    When addHash method is invoked, we are going to create a HashExpression out of "R.A + 4". 
        //    It will work, but we could send "R.A + 4" at R (anyway this scenario is pretty rare).
        //  If join condition is R.A = S.A + 4 we anyway don't care of RHS
        List<Column> columns = ParserUtil.getJSQLColumns(joinExprs);
        for(Column column: columns){
            String exprStr = ParserUtil.getStringExpr(column);
            if(_nt.contains(_inputTupleSchema, exprStr)){
                exprList.add(column);
                //we have to add it even if this is a joinCondition for this component
                //  because creating Hashes goes after ProjectOperator
            }
        }
    }

    /*
     * Expressions from exprList are all appeapring somewhere in the query plan
     *   This method never can raise an exception, it can only cause suboptimality
     *   For example, if I have inputTupleSchema "R.A, R.A + R.B" in R and exprList "R.A, R.A + R.B"
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
                    if(aloneColumnNames.contains(columnStr)){
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

    private List<ColumnNameType> createSchema(List<Expression> choosenExprs) {
        List<ColumnNameType> result = new ArrayList<ColumnNameType>();

        for(Expression expr: choosenExprs){
            List<Column> columns = ParserUtil.getJSQLColumns(expr);
            Column column = columns.get(0);

            TypeConversion tc = ParserUtil.getColumnType(column, _tan, _schema);

            //attach the TC of the first column
            String exprStr = ParserUtil.getStringExpr(expr);
            ColumnNameType cnt = new ColumnNameType(exprStr, tc);
            result.add(cnt);
        }

        return result;
    }

}