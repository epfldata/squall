package sql.optimizers.cost;

import java.util.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.statement.select.SelectItem;
import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import plan_runner.queryPlans.QueryPlan;
import plan_runner.utilities.SystemParameters;
import sql.optimizers.CompGen;
import sql.schema.Schema;
import sql.util.HierarchyExtractor;
import sql.util.ParserUtil;
import sql.util.TupleSchema;
import sql.visitors.jsql.SQLVisitor;
import sql.visitors.squall.NameJoinHashVisitor;
import sql.visitors.squall.NameSelectItemsVisitor;
import sql.visitors.squall.NameWhereVisitor;

/*
 * It is necessary that this class operates with Tables,
 *   since we don't want multiple CG sharing the same copy of DataSourceComponent.
 */
public class NameCompGen implements CompGen{
    private SQLVisitor _pq;

    private Map _map;
    private Schema _schema;
    private String _dataPath;
    private String _extension;

    private QueryPlan _queryPlan = new QueryPlan();
    //List of Components which are already added throughEquiJoinComponent and OperatorComponent
    private List<Component> _subPlans = new ArrayList<Component>();

    //compName, CostParams for all the components from _queryPlan
    private Map<String, CostParams> _compCost =  new HashMap<String, CostParams>();

    private CostEstimator _costEst;

    //used for WHERE clause
    private final Map<String, Expression> _compNamesAndExprs;
    private final Map<Set<String>, Expression> _compNamesOrExprs;

    //used for Projection
    private final ProjGlobalCollect _globalCollect;

    public NameCompGen(Schema schema,
            SQLVisitor pq,
            Map map,
            //called from CostOptimizer, which already has CPA instantiated
            CostParallelismAssigner parAssigner,
            Map<String, Expression> compNamesAndExprs,
            Map<Set<String>, Expression> compNamesOrExprs,
            ProjGlobalCollect globalCollect){
        _pq = pq;
        _map = map;
        _schema = schema;
        
        _dataPath = SystemParameters.getString(map, "DIP_DATA_PATH");
        _extension = SystemParameters.getString(map, "DIP_EXTENSION");
        
        if(parAssigner != null){
            _costEst = new CostEstimator(schema, pq, _compCost, parAssigner);
        }

        _compNamesAndExprs = compNamesAndExprs;
        _compNamesOrExprs = compNamesOrExprs;

        _globalCollect = globalCollect;
    }   

    public CostParams getCostParameters(String componentName){
        return _compCost.get(componentName);
    }

    @Override
    public QueryPlan getQueryPlan(){
        return _queryPlan;
    }

    @Override
    public List<Component> getSubPlans(){
        return _subPlans;
    }

    /*
     * adding a DataSourceComponent to the list of components
     */
    @Override
    public DataSourceComponent generateDataSource(String tableCompName){
        DataSourceComponent source = createAddDataSource(tableCompName);
        
        createCompCost(source);
        if(_costEst!=null) _costEst.setInputParams(source);
        
        //operators
        addSelectOperator(source);
        addProjectOperator(source);

        if(_costEst!=null) _costEst.setOutputParamsAndPar(source);

        return source;
    }

    private DataSourceComponent createAddDataSource(String tableCompName) {
        String tableSchemaName = _pq.getTan().getSchemaName(tableCompName);
        String sourceFile = tableSchemaName.toLowerCase();

        DataSourceComponent relation = new DataSourceComponent(
                                        tableCompName,
                                        _dataPath + sourceFile + _extension,
                                        _queryPlan);
        _subPlans.add(relation);
        return relation;
    }

    /*
     * Setting schema for DataSourceComponent
     */
    private void createCompCost(DataSourceComponent source) {
        String compName = source.getName();
        String schemaName = _pq.getTan().getSchemaName(compName);
        CostParams costParams = new CostParams();

        //schema is consisted of TableAlias.columnName
        costParams.setSchema(ParserUtil.createAliasedSchema(_schema.getTableSchema(schemaName), compName));                
        
        _compCost.put(compName, costParams);
    }    
    
    /*
     * Join between two components
     * List<Expression> is a set of join conditions between two components.
     */
    @Override
    public EquiJoinComponent generateEquiJoin(Component left, Component right){
        EquiJoinComponent joinComponent = createAndAddEquiJoin(left, right);
        
        //compute join condition
        List<Expression> joinCondition = ParserUtil.getJoinCondition(_pq, left, right);
        
        //set hashes for two parents, has to be before createCompCost
        addHash(left, joinCondition);
        addHash(right, joinCondition);        
        
        createCompCost(joinComponent);
        if(_costEst!=null) _costEst.setInputParams(joinComponent, joinCondition);
        
        //operators
        addSelectOperator(joinComponent);
        
        //TODO decomment when NSIV.visit(Column) is fixed 
        //  - issue in TPCH9
        //if(!ParserUtil.isFinalJoin(joinComponent, _pq)){
        addProjectOperator(joinComponent);
            //assume no operators between projection and final aggregation
            // final aggregation is able to do projection in GroupByProjection
        //}
            
        if(ParserUtil.isFinalJoin(joinComponent, _pq)){
            //final component in terms of joins
            addFinalAgg(joinComponent);
        }

        if(_costEst!=null) _costEst.setOutputParamsAndPar(joinComponent);

        return joinComponent;
    }

    private EquiJoinComponent createAndAddEquiJoin(Component left, Component right){
        EquiJoinComponent joinComponent = new EquiJoinComponent(
                    left,
                    right,
                    _queryPlan);

        _subPlans.remove(left);
        _subPlans.remove(right);
        _subPlans.add(joinComponent);

        return joinComponent;
    }

    /*
     * This can estimate selectivity/cardinality of a join between between any two components
     *   but with a restriction - rightParent has only one component mentioned in joinCondition.
     *   If connection between any components is allowed,
     *     we have to find a way combining multiple distinct selectivities
     *     (for example having a component R-S and T-V, how to combine R.A=T.A and S.B=V.B?)
     * This method is based on usual way to join tables - on their appropriate keys.
     * It works for cyclic queries as well (TPCH5 is an example).
     */
    private void createCompCost(EquiJoinComponent joinComponent) {
        //create schema and selectivity wrt leftParent
        String compName = joinComponent.getName();
        CostParams costParams = new CostParams();

        //*********set schema
        TupleSchema schema = ParserUtil.joinSchema(joinComponent.getParents(), _compCost);
        costParams.setSchema(schema);
        
        _compCost.put(compName, costParams);
    }
    
    /*************************************************************************************
     * WHERE clause - SelectOperator
     *************************************************************************************/
    private void addSelectOperator(Component component){
        Expression whereCompExpr = createWhereForComponent(component);

        processWhereForComponent(component, whereCompExpr);
        if(_costEst!=null) _costEst.processWhereCost(component, whereCompExpr);
    }

    /*
     * Merging atomicExpr and orExpressions corresponding to this component
     */
    private Expression createWhereForComponent(Component component){
        Expression expr = _compNamesAndExprs.get(component.getName());

        for(Map.Entry<Set<String>, Expression> orEntry: _compNamesOrExprs.entrySet()){
            Set<String> orCompNames = orEntry.getKey();

            //TODO: the full solution would be that OrExpressions are splitted into subexpressions
            //  which might be executed on their LCM
            //  Not implemented because it's quite rare - only TPCH7
            //  Even in TPCH7 there is no need for multiple LCM.
            //TODO: selectivityEstimation for pushing OR need to be improved
            Expression orExpr = orEntry.getValue();
            if(HierarchyExtractor.isLCM(component, orCompNames)){
                expr = appendAnd(expr, orExpr);
            }else if(component instanceof DataSourceComponent){
                DataSourceComponent source = (DataSourceComponent) component;
                Expression addedExpr = getMineSubset(source, orExpr);
                expr = appendAnd(expr, addedExpr);
            }
        }
        return expr;
    }
    
    private Expression appendAnd(Expression fullExpr, Expression atomicExpr){
        if(atomicExpr != null){
            if (fullExpr != null){
                //appending to previous expressions
                fullExpr = new AndExpression(fullExpr, atomicExpr);
            }else{
                //this is the first expression for this component
                fullExpr = atomicExpr;
            }
        }
        return fullExpr;
    }
    
    /*
     * get a list of WhereExpressions (connected by OR) belonging to source
     * For example (N1.NATION = FRANCE AND N2.NATION = GERMANY) OR (N1.NATION = GERMANY AND N2.NATION = FRANCE)
     *   returns N1.NATION = FRANCE OR N1.NATION = GERMANY
     */
    public Expression getMineSubset(DataSourceComponent source, Expression expr){
        List<String> compNames = ParserUtil.getCompNamesFromColumns(ParserUtil.getJSQLColumns(expr));
        
        boolean mine = true;
        for(String compName: compNames){
            if(!compName.equals(source.getName())){
                mine = false;
                break;
            }
        }
        
        if(mine){
            return expr;
        }
        
        Expression result = null;
        if(expr instanceof OrExpression || expr instanceof AndExpression){
            BinaryExpression be = (BinaryExpression) expr;
            result = appendOr(result, getMineSubset(source, be.getLeftExpression()));
            result = appendOr(result, getMineSubset(source, be.getRightExpression()));
        }else if(expr instanceof Parenthesis){
            Parenthesis prnth = (Parenthesis) expr;
            result = getMineSubset(source, prnth.getExpression());
        }
        
        //whatever is not fully recognized (all the compNames = source), and is not And or Or, returns null
        return result;
    }
    
    private Expression appendOr(Expression fullExpr, Expression atomicExpr){
        if(atomicExpr != null){
            if (fullExpr != null){
                //appending to previous expressions
                fullExpr = new OrExpression(fullExpr, atomicExpr);
            }else{
                //this is the first expression for this component
                fullExpr = atomicExpr;
            }
        }
        return fullExpr;
    }    


    /*
     * whereCompExpression is the part of WHERE clause which refers to affectedComponent
     * This is the only method in this class where IndexWhereVisitor is actually instantiated and invoked
     * 
     * SelectOperator is able to deal with ValueExpressions (and not only with ColumnReferences),
     *   but here we recognize JSQL expressions here which can be built of inputTupleSchema (constants included)
     */
    private void processWhereForComponent(Component affectedComponent, Expression whereCompExpr){
        if(whereCompExpr != null){
            //first get the current schema of the component
            TupleSchema tupleSchema = _compCost.get(affectedComponent.getName()).getSchema();
            NameWhereVisitor whereVisitor = new NameWhereVisitor(tupleSchema, affectedComponent);
            whereCompExpr.accept(whereVisitor);
            attachWhereClause(affectedComponent, whereVisitor.getSelectOperator());
        }
    }

    private void attachWhereClause(Component affectedComponent, SelectOperator select) {
        affectedComponent.addOperator(select);
    }


    /*************************************************************************************
     * Project operator
     *************************************************************************************/
    private void addProjectOperator(Component component){
        String compName = component.getName();
        TupleSchema inputTupleSchema = _compCost.get(compName).getSchema();
        ProjSchemaCreator psc = new ProjSchemaCreator(_globalCollect, inputTupleSchema, component, _pq, _schema);
        psc.create();

        TupleSchema outputTupleSchema = psc.getOutputSchema();
        
        if(!ParserUtil.isSameSchema(inputTupleSchema, outputTupleSchema)){
            //no need to add projectOperator unless it changes something
            attachProjectOperator(component, psc.getProjectOperator());
            processProjectCost(component, outputTupleSchema);
        }
    }

    private void attachProjectOperator(Component component, ProjectOperator project){
        component.addOperator(project);
    }

    private void processProjectCost(Component component, TupleSchema outputTupleSchema){
        //only schema is changed
        String compName = component.getName();
        _compCost.get(compName).setSchema(outputTupleSchema);
    }

    /*************************************************************************************
     * SELECT clause - Final aggregation
     *************************************************************************************/
     private void addFinalAgg(Component lastComponent) {
        //TODO: take care in nested case
        TupleSchema tupleSchema = _compCost.get(lastComponent.getName()).getSchema();
        NameSelectItemsVisitor selectVisitor = new NameSelectItemsVisitor(tupleSchema, _map, lastComponent);
        for(SelectItem elem: _pq.getSelectItems()){
            elem.accept(selectVisitor);
        }
        List<AggregateOperator> aggOps = selectVisitor.getAggOps();
        List<ValueExpression> groupByVEs = selectVisitor.getGroupByVEs();
        List<Expression> groupByExprs = selectVisitor.getGroupByExprs();

        attachSelectClause(lastComponent, aggOps, groupByVEs, groupByExprs);
    }

    private void attachSelectClause(Component lastComponent, List<AggregateOperator> aggOps, List<ValueExpression> groupByVEs, List<Expression> groupByExprs) {
        ProjectOperator project = new ProjectOperator(groupByVEs);
        if (aggOps.isEmpty()){
            lastComponent.addOperator(project);
        }else if (aggOps.size() == 1){
            //all the others are group by
            AggregateOperator firstAgg = aggOps.get(0);
            firstAgg.setGroupByProjection(project);
            
            if(firstAgg.getDistinct() == null){
                lastComponent.addOperator(firstAgg);
            }else{
                //Setting new level of components is only necessary for distinct in aggregates
                
                //in general groupByVEs is not a ColumnReference (it can be an addition, for example). 
                //  ProjectOperator is not obliged to create schema which fully fits in what FinalAggregation wants
                addHash(lastComponent, groupByExprs);
                OperatorComponent newComponent = new OperatorComponent(lastComponent,
                                                                 ParserUtil.generateUniqueName("OPERATOR"),
                                                                 _queryPlan).addOperator(firstAgg);    
                //we can use the same firstAgg, because we no tupleSchema change occurred after LAST_COMPONENT:FinalAgg and NEW_COMPONENT:FinalAgg
                //  Namely, NEW_COMPONENT has only FinalAgg operator
            }
        }else{
            throw new RuntimeException("For now only one aggregate function supported!");
        }
    }


    /*************************************************************************************
     * HASH
     *************************************************************************************/

    //set hash for this component, knowing its position in the query plan.
    //  Conditions are related only to parents of join,
    //  but we have to filter who belongs to my branch in NameJoinHashVisitor.
    //  We don't want to hash on something which will be used to join with same later component in the hierarchy.
    private void addHash(Component component, List<Expression> joinCondition) {
        TupleSchema tupleSchema = _compCost.get(component.getName()).getSchema();
        NameJoinHashVisitor joinOn = new NameJoinHashVisitor(tupleSchema, component);
        for(Expression exp: joinCondition){
            exp.accept(joinOn);
        }
        List<ValueExpression> hashExpressions = joinOn.getExpressions();

        //if joinCondition is a R.A + 5 = S.A, and inputTupleSchema is "R.A + 5", this is NOT a complex condition
        //  HashExpression is a ColumnReference(0)
        if(!joinOn.isComplexCondition()){
            //all the join conditions are represented through columns, no ValueExpression
            //guaranteed that both joined components will have joined columns visited in the same order
            //i.e R.A=S.A and R.B=S.B, the columns are (R.A, R.B), (S.A, S.B), respectively
            List<Integer> hashIndexes = ParserUtil.extractColumnIndexes(hashExpressions);

            //hash indexes in join condition
            component.setHashIndexes(hashIndexes);
        }else{
            //hash expressions in join condition
            component.setHashExpressions(hashExpressions);
        }
    }
}