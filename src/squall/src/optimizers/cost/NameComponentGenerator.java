/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers.cost;

import optimizers.*;
import components.Component;
import components.DataSourceComponent;
import components.EquiJoinComponent;
import components.OperatorComponent;
import conversion.TypeConversion;
import estimators.ConfigSelectivityEstimator;
import estimators.SelingerSelectivityEstimator;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import operators.AggregateOperator;
import operators.ProjectOperator;
import operators.SelectOperator;
import queryPlans.QueryPlan;
import schema.ColumnNameType;
import schema.Schema;
import util.HierarchyExtractor;
import util.ParserUtil;
import util.TableAliasName;
import utilities.DeepCopy;
import visitors.squall.IndexSelectItemsVisitor;
import visitors.squall.NameJoinHashVisitor;
import visitors.squall.NameSelectItemsVisitor;
import visitors.squall.NameWhereVisitor;

/*
 * It is necessary that this class operates with Tables,
 *   since we don't want multiple CG sharing the same copy of DataSourceComponent.
 */
public class NameComponentGenerator implements ComponentGenerator{
    private TableAliasName _tan;
    private Translator _ot;

    private Schema _schema;
    private String _dataPath;
    private String _extension;
    private Map _map;

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

    public NameComponentGenerator(Schema schema,
            TableAliasName tan,
            NameTranslator ot,
            String dataPath,
            String extension,
            Map map,
            //called from CostOptimizer, which already has CostParallellismAssigner instantiated
            CostParallelismAssigner parAssigner,
            Map<String, Expression> compNamesAndExprs,
            Map<Set<String>, Expression> compNamesOrExprs){
        _schema = schema;
        _tan = tan;
        _ot = new NameTranslator();
        _dataPath = dataPath;
        _extension = extension;
        _map = map;
        _parAssigner = parAssigner;

        _fileEstimator = new ConfigSelectivityEstimator(map);
        _selEstimator = new SelingerSelectivityEstimator(schema, tan);

        _compNamesAndExprs = compNamesAndExprs;
        _compNamesOrExprs = compNamesOrExprs;
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

        return source;
    }

    private DataSourceComponent createAddDataSource(String tableCompName) {
        String tableSchemaName = _tan.getSchemaName(tableCompName);
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
        String schemaName = _tan.getSchemaName(compName);
        CostParams costParams = new CostParams();

        costParams.setCardinality(_schema.getTableSize(schemaName));
        //schema is consisted of TableAlias.columnName
        costParams.setSchema(createAliasedSchema(_schema.getTableSchema(schemaName), compName));
                
        _compCost.put(compName, costParams);
    }

    /*
     * From a list of <NATIONNATE, StringConversion>
     *   it creates a list of <N1.NATIONNAME, StringConversion>
     */
    private List<ColumnNameType> createAliasedSchema(List<ColumnNameType> originalSchema, String aliasName){
        List<ColumnNameType> result = new ArrayList<ColumnNameType>();

        for(ColumnNameType cnt: originalSchema){
            String name = cnt.getName();
            name = aliasName + "." + name;
            TypeConversion tc = cnt.getType();
            result.add(new ColumnNameType(name, tc));
        }

        return result;
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
        //set hashes for two parents, has to be after all the operators
        addHash(left, joinCondition);
        addHash(right, joinCondition);

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

    private void createCompCost(EquiJoinComponent joinComponent, List<Expression> joinCondition) {
        //create schema and selectivity wrt leftParent
        String compName = joinComponent.getName();
        CostParams costParams = new CostParams();
        Component[] parents = joinComponent.getParents();

        //set schema
        List<ColumnNameType> schema = ParserUtil.joinSchema(joinComponent.getParents(), _compCost);
        costParams.setSchema(schema);

        //********* set selectivity
        List<Column> joinColumns = ParserUtil.getJSQLColumns(joinCondition);
        List<String> joinCompNames = ParserUtil.getCompNamesFromColumns(joinColumns);

        String leftJoinTableSchemaName = getJoinSchemaName(joinCompNames, parents[0]);
        String rightJoinTableSchemaName = getJoinSchemaName(joinCompNames, parents[1]);
        
        double leftCardinality = _compCost.get(parents[0].getName()).getCardinality();
        double rightCardinality = _compCost.get(parents[1].getName()).getCardinality();
        double selectivity;
        if(leftJoinTableSchemaName.equals(rightJoinTableSchemaName)){
            //we treat this as a cross-product on which some selections are performed
            //IMPORTANT: selectivity is the output/input rate in the case of EquiJoin
            selectivity = (leftCardinality * rightCardinality) / (leftCardinality + rightCardinality);
        }else{
            double ratio = _schema.getRatio(leftJoinTableSchemaName, rightJoinTableSchemaName);
            selectivity = (leftCardinality * ratio) / (leftCardinality + rightCardinality);
        }
        costParams.setSelectivity(selectivity);
        //*********
        
        _compCost.put(compName, costParams);
    }

    /*
     * @allJoinCompNames - all the component names from the join condition
     * joinCompNames - all the component names from the join condition corresponding to parent
     */
    private String getJoinSchemaName(List<String> allJoinCompNames, Component parent) {
        List<DataSourceComponent> ancestorComps = parent.getAncestorDataSources();
        List<String> ancestors = ParserUtil.getSourceNameList(ancestorComps);
        List<String> joinCompNames = ParserUtil.getIntersection(allJoinCompNames, ancestors);
        if(joinCompNames.size() > 1){
            throw new RuntimeException("Cannot estimate join selectivity if a query is cyclic!");
        }
        return _tan.getSchemaName(joinCompNames.get(0));
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
        NameWhereVisitor whereVisitor = new NameWhereVisitor(_schema, _tan, tupleSchema);
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
     * SELECT clause - Final aggregation
     *************************************************************************************/
     public void addFinalAgg(List<SelectItem> selectItems) {
        //TODO: take care in nested case
        Component lastComponent = _queryPlan.getLastComponent();
        List<ColumnNameType> tupleSchema = _compCost.get(lastComponent.getName()).getSchema();
        NameSelectItemsVisitor selectVisitor = new NameSelectItemsVisitor(_schema, _tan, tupleSchema, _map);
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
                boolean newLevel = !(_ot.isHashedBy(lastComponent, groupByColumns));
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
        NameJoinHashVisitor joinOn = new NameJoinHashVisitor(_schema, _tan, tupleSchema, component);
        for(Expression exp: joinCondition){
            exp.accept(joinOn);
        }
        List<ValueExpression> hashExpressions = joinOn.getExpressions();

        if(!joinOn.isComplexCondition()){
            //all the join conditions are represented through columns, no ValueExpression (neither in joined component)
            //guaranteed that both joined components will have joined columns visited in the same order
            //i.e R.A=S.A and R.B=S.B, the columns are (R.A, R.B), (S.A, S.B), respectively
            List<Integer> hashIndexes = ParserUtil.extractColumnIndexes(hashExpressions);

            //hash indexes in join condition
            component.setHashIndexes(hashIndexes);
        }else{
            //hahs expressions in join condition
            component.setHashExpressions(hashExpressions);
        }
    }
}