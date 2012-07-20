package sql.optimizers.cost;

import plan_runner.components.Component;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import sql.estimators.ConfigSelectivityEstimator;
import sql.estimators.SelingerSelectivityEstimator;
import plan_runner.expressions.ValueExpression;
import java.util.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.operators.SelectOperator;
import sql.optimizers.ComponentGenerator;
import plan_runner.queryPlans.QueryPlan;
import sql.schema.ColumnNameType;
import sql.schema.Schema;
import sql.util.HierarchyExtractor;
import sql.util.ParserUtil;
import plan_runner.utilities.SystemParameters;
import sql.visitors.jsql.SQLVisitor;
import sql.visitors.squall.NameJoinHashVisitor;
import sql.visitors.squall.NameSelectItemsVisitor;
import sql.visitors.squall.NameWhereVisitor;

/*
 * It is necessary that this class operates with Tables,
 *   since we don't want multiple CG sharing the same copy of DataSourceComponent.
 */
public class NameComponentGenerator implements ComponentGenerator{
    private SQLVisitor _pq;
    private NameTranslator _nt = new NameTranslator();

    private Map _map;
    private Schema _schema;
    private String _dataPath;
    private String _extension;

    private QueryPlan _queryPlan = new QueryPlan();
    //List of Components which are already added throughEquiJoinComponent and OperatorComponent
    private List<Component> _subPlans = new ArrayList<Component>();

    //compName, CostParams for all the components from _queryPlan
    private Map<String, CostParams> _compCost =  new HashMap<String, CostParams>();

    //needed because NameComponentGenerator sets parallelism for all the components
    private final CostParallelismAssigner _parAssigner;
    private boolean _isForSourcesOnly = false; //instantiated from CostParallelismAssigner

    private ConfigSelectivityEstimator _fileEstimator;
    private SelingerSelectivityEstimator _selEstimator;

    //used for WHERE clause
    private final Map<String, Expression> _compNamesAndExprs;
    private final Map<Set<String>, Expression> _compNamesOrExprs;

    //used for Projection
    private final ProjGlobalCollect _globalCollect;

    public NameComponentGenerator(Schema schema,
            SQLVisitor pq,
            Map map,
            //called from CostOptimizer, which already has CostParallellismAssigner instantiated
            CostParallelismAssigner parAssigner,
            Map<String, Expression> compNamesAndExprs,
            Map<Set<String>, Expression> compNamesOrExprs,
            ProjGlobalCollect globalCollect){
        _pq = pq;
        _map = map;
        _schema = schema;
        
        _dataPath = SystemParameters.getString(map, "DIP_DATA_PATH");
        _extension = SystemParameters.getString(map, "DIP_EXTENSION");
        
        _parAssigner = parAssigner;

        _fileEstimator = new ConfigSelectivityEstimator(map);
        _selEstimator = new SelingerSelectivityEstimator(schema, _pq.getTan());

        _compNamesAndExprs = compNamesAndExprs;
        _compNamesOrExprs = compNamesOrExprs;

        _globalCollect = globalCollect;
    }
    
    public NameComponentGenerator(Schema schema,
            SQLVisitor pq,
            Map map,
            //called from CostOptimizer, which already has CostParallellismAssigner instantiated
            CostParallelismAssigner parAssigner,
            Map<String, Expression> compNamesAndExprs,
            Map<Set<String>, Expression> compNamesOrExprs,
            ProjGlobalCollect globalCollect,
            boolean isForSourcesOnly){
        this(schema, pq, map, parAssigner, compNamesAndExprs, compNamesOrExprs, globalCollect);
        _isForSourcesOnly = isForSourcesOnly;
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
        
        //operators
        addSelectOperator(source);
        addProjectOperator(source);

        finalizeCompCost(source);
        
        //setting parallelism if this is not used for determining source parallelism
        if(!_isForSourcesOnly){
            _parAssigner.setParallelism(source, _compCost);
        }

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
     * Setting cardinality and schema for DataSourceComponent
     */
    private void createCompCost(DataSourceComponent source) {
        String compName = source.getName();
        String schemaName = _pq.getTan().getSchemaName(compName);
        CostParams costParams = new CostParams();

        costParams.setCardinality(_schema.getTableSize(schemaName));
        //schema is consisted of TableAlias.columnName
        costParams.setSchema(ParserUtil.createAliasedSchema(_schema.getTableSchema(schemaName), compName));
                
        _compCost.put(compName, costParams);
    }

    /*
     * Works for both DataSource and EquiJoin component
     */
    private void finalizeCompCost(Component comp){
        CostParams costParams = _compCost.get(comp.getName());
        long currentCardinality = costParams.getCardinality();
        double selectivity = costParams.getSelectivity();
        long cardinality = (long) (selectivity * currentCardinality);
        costParams.setCardinality(cardinality);
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
        
        createCompCost(joinComponent, joinCondition);
        
        //operators
        addSelectOperator(joinComponent);
        if(!ParserUtil.isFinalJoin(joinComponent, _pq)){
            addProjectOperator(joinComponent);
            //assume no operators between projection and final aggregation
            // final aggregation is able to do projection in GroupByProjection
        }
        if(ParserUtil.isFinalJoin(joinComponent, _pq)){
            //final component in terms of joins
            addFinalAgg(joinComponent);
        }

        finalizeCompCost(joinComponent);
        //setting parallelism
        _parAssigner.setParallelism(joinComponent, _compCost);

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
     *   but with a restriction - righParent has only one component mentioned in joinCondition.
     *   If connection between any components is allowed,
     *     we have to find a way combining multiple distinct selectivities
     *     (for example having a component R-S and T-V, how to combine R.A=T.A and S.B=V.B?)
     * This method is based on usual way to join tables - on their appropriate keys.
     * It works for cyclic queries as well (TPCH5 is an example).
     */
    private void createCompCost(EquiJoinComponent joinComponent, List<Expression> joinCondition) {
        //create schema and selectivity wrt leftParent
        String compName = joinComponent.getName();
        CostParams costParams = new CostParams();
        Component[] parents = joinComponent.getParents();

        //*********set schema
        List<ColumnNameType> schema = ParserUtil.joinSchema(joinComponent.getParents(), _compCost);
        costParams.setSchema(schema);

        //********* set initial (join) selectivity and initial cardinality
        long leftCardinality = _compCost.get(parents[0].getName()).getCardinality();
        long rightCardinality = _compCost.get(parents[1].getName()).getCardinality();
        double rightSelectivity = _compCost.get(parents[1].getName()).getSelectivity();

        //compute
        long inputCardinality = leftCardinality + rightCardinality;
        double selectivity = computeSelectivity(joinComponent, joinCondition, leftCardinality, rightCardinality, rightSelectivity);

        //setting
        costParams.setCardinality(inputCardinality);
        costParams.setSelectivity(selectivity);
        //*********
        
        _compCost.put(compName, costParams);
    }

    private double computeSelectivity(EquiJoinComponent joinComponent, List<Expression> joinCondition, 
            long leftCardinality, long rightCardinality, double rightSelectivity){

        Component[] parents = joinComponent.getParents();
        double selectivity = 1;

        List<Column> joinColumns = ParserUtil.getJSQLColumns(joinCondition);
        List<String> joinCompNames = ParserUtil.getCompNamesFromColumns(joinColumns);

        List<String> leftJoinTableSchemaNames = getJoinSchemaNames(joinCompNames, parents[0]);
        List<String> rightJoinTableSchemaNames = getJoinSchemaNames(joinCompNames, parents[1]);

        if(rightJoinTableSchemaNames.size() > 1){
            throw new RuntimeException("Currently, this support only lefty plans!");
        }
        String rightJoinTableSchemaName = rightJoinTableSchemaNames.get(0);
        
        for(String leftJoinTableSchemaName: leftJoinTableSchemaNames){
            selectivity = selectivity * 
                    computeHashSelectivity(leftJoinTableSchemaName, rightJoinTableSchemaName, leftCardinality, rightCardinality, rightSelectivity);
        }

        return selectivity;
    }

    /*
     * @allJoinCompNames - all the component names from the join condition
     * joinCompNames - all the component names from the join condition corresponding to parent
     */
    private List<String> getJoinSchemaNames(List<String> allJoinCompNames, Component parent) {
        List<String> ancestors = ParserUtil.getSourceNameList(parent);
        List<String> joinCompNames = ParserUtil.getIntersection(allJoinCompNames, ancestors);

        List<String> joinSchemaNames = new ArrayList<String>();
        for(String joinCompName: joinCompNames){
            joinSchemaNames.add(_pq.getTan().getSchemaName(joinCompName));
        }
        return joinSchemaNames;
    }

    private double computeHashSelectivity(String leftJoinTableSchemaName, String rightJoinTableSchemaName, 
            long leftCardinality, long rightCardinality, double rightSelectivity){
        long inputCardinality = leftCardinality + rightCardinality;
        double selectivity;

        if(leftJoinTableSchemaName.equals(rightJoinTableSchemaName)){
            //we treat this as a cross-product on which some selections are performed
            //IMPORTANT: selectivity is the output/input rate in the case of EquiJoin
            selectivity = (leftCardinality * rightCardinality) / inputCardinality;
        }else{
            double ratio = _schema.getRatio(leftJoinTableSchemaName, rightJoinTableSchemaName);
            selectivity = (leftCardinality * ratio * rightSelectivity) / inputCardinality;
        }
        return selectivity;
    }

    /*************************************************************************************
     * WHERE clause - SelectOperator
     *************************************************************************************/
    private void addSelectOperator(Component component){
        Expression whereCompExpr = createWhereForComponent(component);

        processWhereForComponent(component, whereCompExpr);
        processWhereCost(component, whereCompExpr);
    }

    /*
     * Merging atomicExpr and orExpressions corresponding to this component
     */
    private Expression createWhereForComponent(Component component){
        Expression expr = _compNamesAndExprs.get(component.getName());

        for(Map.Entry<Set<String>, Expression> orEntry: _compNamesOrExprs.entrySet()){
            Set<String> orCompNames = orEntry.getKey();

            if(HierarchyExtractor.isLCM(component, orCompNames)){
                Expression orExpr = orEntry.getValue();
                if (expr != null){
                    //appending to previous expressions
                    expr = new AndExpression(expr, orExpr);
                }else{
                    //this is the first expression for this component
                    expr = orExpr;
                }
            }
        }
        return expr;
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
            List<ColumnNameType> tupleSchema = _compCost.get(affectedComponent.getName()).getSchema();
            NameWhereVisitor whereVisitor = new NameWhereVisitor(_schema, _pq.getTan(), tupleSchema);
            whereCompExpr.accept(whereVisitor);
            attachWhereClause(affectedComponent, whereVisitor.getSelectOperator());
        }
    }

    private void attachWhereClause(Component affectedComponent, SelectOperator select) {
        affectedComponent.addOperator(select);
    }

    private void processWhereCost(Component component, Expression whereCompExpr) {
        if(whereCompExpr != null){
            //this is going to change selectivity
            String compName = component.getName();
            CostParams costParams = _compCost.get(compName);
            double previousSelectivity = costParams.getSelectivity();

            double selectivity = previousSelectivity * _selEstimator.estimate(whereCompExpr);
            costParams.setSelectivity(selectivity);
        }
    }


    /*************************************************************************************
     * Project operator
     *************************************************************************************/
    private void addProjectOperator(Component component){
        String compName = component.getName();
        List<ColumnNameType> inputTupleSchema = _compCost.get(compName).getSchema();
        ProjSchemaCreator psc = new ProjSchemaCreator(_globalCollect, inputTupleSchema, component, _pq, _schema);
        psc.create();

        List<ColumnNameType> outputTupleSchema = psc.getOutputSchema();
        
        if(!ParserUtil.isSameSchema(inputTupleSchema, outputTupleSchema)){
            //no need to add projectOperator unless it changes something
            attachProjectOperator(component, psc.getProjectOperator());
            processProjectCost(component, outputTupleSchema);
        }
    }

    private void attachProjectOperator(Component component, ProjectOperator project){
        component.addOperator(project);
    }

    private void processProjectCost(Component component, List<ColumnNameType> outputTupleSchema){
        //only schema is changed
        String compName = component.getName();
        _compCost.get(compName).setSchema(outputTupleSchema);
    }

    /*************************************************************************************
     * SELECT clause - Final aggregation
     *************************************************************************************/
     private void addFinalAgg(Component lastComponent) {
        //TODO: take care in nested case
        List<ColumnNameType> tupleSchema = _compCost.get(lastComponent.getName()).getSchema();
        NameSelectItemsVisitor selectVisitor = new NameSelectItemsVisitor(_schema, _pq.getTan(), tupleSchema, _map);
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
        List<ColumnNameType> tupleSchema = _compCost.get(component.getName()).getSchema();
        NameJoinHashVisitor joinOn = new NameJoinHashVisitor(_schema, _pq.getTan(), tupleSchema, component);
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