package optimizers.cost;

import components.Component;
import components.DataSourceComponent;
import components.EquiJoinComponent;
import components.OperatorComponent;
import estimators.ConfigSelectivityEstimator;
import estimators.SelingerSelectivityEstimator;
import expressions.ValueExpression;
import java.util.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import operators.AggregateOperator;
import operators.ProjectOperator;
import operators.SelectOperator;
import optimizers.ComponentGenerator;
import queryPlans.QueryPlan;
import schema.ColumnNameType;
import schema.Schema;
import util.HierarchyExtractor;
import util.ParserUtil;
import utilities.SystemParameters;
import visitors.jsql.SQLVisitor;
import visitors.squall.NameJoinHashVisitor;
import visitors.squall.NameSelectItemsVisitor;
import visitors.squall.NameWhereVisitor;

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

    //needed because NameComponentGenerator creates parallelism for EquiJoinComponent
    private final CostParallelismAssigner _parAssigner;

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
    public Component generateEquiJoin(Component left, Component right, List<Expression> joinCondition){
        EquiJoinComponent joinComponent = createAndAddEquiJoin(left, right);
        createCompCost(joinComponent, joinCondition);
        
        //operators
        addSelectOperator(joinComponent);
        addProjectOperator(joinComponent);
        //set hashes for two parents, has to be after all the operators
        addHash(left, joinCondition);
        addHash(right, joinCondition);

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
        List<DataSourceComponent> ancestorComps = parent.getAncestorDataSources();
        List<String> ancestors = ParserUtil.getSourceNameList(ancestorComps);
        List<String> joinCompNames = ParserUtil.getIntersection(allJoinCompNames, ancestors);

        List<String> joinSchemaNames = new ArrayList<String>();
        for(String joinCompName: joinCompNames){
            joinSchemaNames.add(_pq.getTan().getSchemaName(joinCompName));
        }
        return joinSchemaNames;
    }

    public double computeHashSelectivity(String leftJoinTableSchemaName, String rightJoinTableSchemaName, 
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
    public void addSelectOperator(Component component){
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
     */
    private void processWhereForComponent(Component affectedComponent, Expression whereCompExpression){
        //first get the current schema of the component
        List<ColumnNameType> tupleSchema = _compCost.get(affectedComponent.getName()).getSchema();
        NameWhereVisitor whereVisitor = new NameWhereVisitor(_schema, _pq.getTan(), tupleSchema);
        whereCompExpression.accept(whereVisitor);
        attachWhereClause(affectedComponent, whereVisitor.getSelectOperator());
    }

    private void attachWhereClause(Component affectedComponent, SelectOperator select) {
        affectedComponent.addOperator(select);
    }

    private void processWhereCost(Component component, Expression whereCompExpr) {
        //this is going to change selectivity
        String compName = component.getName();
        CostParams costParams = _compCost.get(compName);
        double previousSelectivity = costParams.getSelectivity();

        double selectivity = previousSelectivity * _selEstimator.estimate(whereCompExpr);
        costParams.setSelectivity(selectivity);
    }


    /*************************************************************************************
     * Project operator
     *************************************************************************************/
    public void addProjectOperator(Component component){
        String compName = component.getName();
        List<ColumnNameType> tupleSchema = _compCost.get(compName).getSchema();
        ProjSchemaCreator psc = new ProjSchemaCreator(_globalCollect, tupleSchema, component, _pq, _schema);
        psc.create();

        attachProjectOperator(component, psc.getProjectOperator());
        processProjectCost(component, psc.getOutputSchema());
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
     public void addFinalAgg(List<SelectItem> selectItems) {
        //TODO: take care in nested case
        Component lastComponent = _queryPlan.getLastComponent();
        List<ColumnNameType> tupleSchema = _compCost.get(lastComponent.getName()).getSchema();
        NameSelectItemsVisitor selectVisitor = new NameSelectItemsVisitor(_schema, _pq.getTan(), tupleSchema, _map);
        for(SelectItem elem: selectItems){
            elem.accept(selectVisitor);
        }
        List<AggregateOperator> aggOps = selectVisitor.getAggOps();
        List<ValueExpression> groupByVEs = selectVisitor.getGroupByVEs();

        attachSelectClause(lastComponent, aggOps, groupByVEs);
    }

    private void attachSelectClause(Component lastComponent, List<AggregateOperator> aggOps, List<ValueExpression> groupByVEs) {
        if (aggOps.isEmpty()){
            ProjectOperator project = new ProjectOperator(groupByVEs);
            lastComponent.addOperator(project);
        }else if (aggOps.size() == 1){
            //all the others are group by
            AggregateOperator firstAgg = aggOps.get(0);

            if(ParserUtil.isAllColumnRefs(groupByVEs)){
                //plain fields in select
                List<Integer> groupByColumns = ParserUtil.extractColumnIndexes(groupByVEs);
                firstAgg.setGroupByColumns(groupByColumns);

                //Setting new level of components is necessary for correctness only for distinct in aggregates
                    //  but it's certainly pleasant to have the final result grouped on nodes by group by columns.
                boolean newLevel = !(_nt.isHashedBy(lastComponent, groupByColumns));
                if(newLevel){
                    lastComponent.setHashIndexes(groupByColumns);
                    OperatorComponent newComponent = new OperatorComponent(lastComponent,
                                                                      ParserUtil.generateUniqueName("OPERATOR"),
                                                                      _queryPlan).addOperator(firstAgg);

                }else{
                    lastComponent.addOperator(firstAgg);
                }
            }else{
                //on the last component there should be projection added before we add final aggregation
                //  so we should not be here
                throw new RuntimeException("It seems that projection on the last component has not do good job!");
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
    //  but we have to filter who belongs to my branch in IndexJoinHashVisitor.
    //  We don't want to hash on something which will be used to join with same later component in the hierarchy.
    private void addHash(Component component, List<Expression> joinCondition) {
        List<ColumnNameType> tupleSchema = _compCost.get(component.getName()).getSchema();
        NameJoinHashVisitor joinOn = new NameJoinHashVisitor(_schema, _pq.getTan(), tupleSchema, component);
        for(Expression exp: joinCondition){
            exp.accept(joinOn);
        }
        List<ValueExpression> hashExpressions = joinOn.getExpressions();

        //if joinCondition is a R.A + 5 = S.A, and "R.A + 5" is in inputTupleSchema, this is NOT a complex condition
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