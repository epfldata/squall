package ch.epfl.data.sql.optimizers.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;

import org.apache.log4j.Logger;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.OperatorComponent;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.utilities.DeepCopy;
import ch.epfl.data.sql.optimizers.Optimizer;
import ch.epfl.data.sql.schema.Schema;
import ch.epfl.data.sql.util.HierarchyExtractor;
import ch.epfl.data.sql.util.JoinTablesExprs;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.visitors.jsql.AndVisitor;
import ch.epfl.data.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.sql.visitors.squall.IndexSelectItemsVisitor;
import ch.epfl.data.sql.visitors.squall.IndexWhereVisitor;

/*
 * It generates a single query plan, adds a final aggregation,
 *   adds selections (WHERE clause) and do early projections (all unused columns are projected away)
 *
 * Does not take relation cardinalities into account.
 * Assume no projections before the aggregation, so that EarlyProjection may impose some projections.
 * Aggregation only on the last level.
 */
public class IndexRuleOptimizer implements Optimizer {
    private static Logger LOG = Logger.getLogger(IndexRuleOptimizer.class);

    private final Schema _schema;
    private final SQLVisitor _pq;
    private IndexCompGen _cg;
    private final IndexTranslator _it;
    private final Map _map; // map is updates in place

    public IndexRuleOptimizer(Map map) {
	_map = map;
	_pq = ParserUtil.parseQuery(map);

	_schema = new Schema(map);
	_it = new IndexTranslator(_schema, _pq.getTan());
    }

    private void attachSelectClause(Component lastComponent,
	    List<AggregateOperator> aggOps, List<ValueExpression> groupByVEs) {
	if (aggOps.isEmpty()) {
	    final ProjectOperator project = new ProjectOperator(groupByVEs);
	    lastComponent.add(project);
	} else if (aggOps.size() == 1) {
	    // all the others are group by
	    final AggregateOperator firstAgg = aggOps.get(0);

	    if (ParserUtil.isAllColumnRefs(groupByVEs)) {
		// plain fields in select
		final List<Integer> groupByColumns = ParserUtil
			.extractColumnIndexes(groupByVEs);
		firstAgg.setGroupByColumns(groupByColumns);

		// Setting new level of components is necessary for correctness
		// only for distinct in aggregates
		// but it's certainly pleasant to have the final result grouped
		// on nodes by group by columns.
		final boolean newLevel = !(_it.isHashedBy(lastComponent,
			groupByColumns));
		if (newLevel) {
		    lastComponent.setOutputPartKey(groupByColumns);
		    OperatorComponent oc = new OperatorComponent(lastComponent,
			    ParserUtil.generateUniqueName("OPERATOR"))
			    .add(firstAgg);
		    _cg.getQueryBuilder().add(oc);

		} else
		    lastComponent.add(firstAgg);
	    } else {
		// Sometimes groupByVEs contains other functions, so we have to
		// use projections instead of simple groupBy
		// always new level

		// WARNING: groupByVEs cannot be used on two places: that's why
		// we do deep copy
		final ProjectOperator groupByProj = new ProjectOperator(
			(List<ValueExpression>) DeepCopy.copy(groupByVEs));
		if (!(groupByProj.getExpressions() == null || groupByProj
			.getExpressions().isEmpty()))
		    firstAgg.setGroupByProjection(groupByProj);

		// current component
		lastComponent
			.setHashExpressions((List<ValueExpression>) DeepCopy
				.copy(groupByVEs));

		OperatorComponent oc = new OperatorComponent(lastComponent,
			ParserUtil.generateUniqueName("OPERATOR"))
			.add(firstAgg);
		_cg.getQueryBuilder().add(oc);
	    }
	} else
	    throw new RuntimeException(
		    "For now only one aggregate function supported!");
    }

    private void attachWhereClause(Component affectedComponent,
	    SelectOperator select) {
	affectedComponent.add(select);
    }

    private void earlyProjection(QueryBuilder queryPlan) {
	final EarlyProjection early = new EarlyProjection(_schema, _pq.getTan());
	early.operate(queryPlan);
    }

    @Override
    public QueryBuilder generate() {
	_cg = generateTableJoins();

	LOG.info("Before WHERE, SELECT and EarlyProjection: ");
	LOG.info(ParserUtil.toString(_cg.getQueryBuilder()));

	// selectItems might add OperatorComponent, this is why it goes first
	final int queryType = processSelectClause(_pq.getSelectItems());
	processWhereClause(_pq.getWhereExpr());
	if (queryType == IndexSelectItemsVisitor.NON_AGG)
	    LOG.info("Early projection will not be performed since the query is NON_AGG type (contains projections)!");
	else
	    earlyProjection(_cg.getQueryBuilder());

	ParserUtil.orderOperators(_cg.getQueryBuilder());

	final RuleParallelismAssigner parAssign = new RuleParallelismAssigner(
		_cg.getQueryBuilder(), _pq.getTan(), _schema, _map);
	parAssign.assignPar();

	return _cg.getQueryBuilder();
    }

    private IndexCompGen generateTableJoins() {
	final List<Table> tableList = _pq.getTableList();
	final TableSelector ts = new TableSelector(tableList, _schema,
		_pq.getTan());
	final JoinTablesExprs jte = _pq.getJte();

	final IndexCompGen cg = new IndexCompGen(_schema, _pq, _map);

	// first phase
	// make high level pairs
	final List<String> skippedBestTableNames = new ArrayList<String>();
	final int numTables = tableList.size();
	if (numTables == 1) {
	    cg.generateDataSource(ParserUtil.getComponentName(tableList.get(0)));
	    return cg;
	} else {
	    final int highLevelPairs = getNumHighLevelPairs(numTables);

	    for (int i = 0; i < highLevelPairs; i++) {
		final String bestTableName = ts.removeBestTableName();

		// enumerates all the tables it has joinCondition to join with
		final List<String> joinedWith = jte
			.getJoinedWith(bestTableName);
		// dependent on previously used tables, so might return null
		final String bestPairedTable = ts
			.removeBestPairedTableName(joinedWith);
		if (bestPairedTable != null) {
		    // we found a pair
		    final DataSourceComponent bestSource = cg
			    .generateDataSource(bestTableName);
		    final DataSourceComponent bestPairedSource = cg
			    .generateDataSource(bestPairedTable);
		    cg.generateEquiJoin(bestSource, bestPairedSource);
		} else
		    // we have to keep this table for latter processing
		    skippedBestTableNames.add(bestTableName);
	    }
	}

	// second phase
	// join (2-way join components) with unused tables, until there is no
	// more tables
	List<Component> subPlans = cg.getSubPlans();

	/*
	 * Why outer loop is unpairedTables, and inner is subPlans: 1) We first
	 * take care of small tables 2) In general, there is smaller number of
	 * unpaired tables than tables 3) Number of ancestors always grow, while
	 * number of joinedTables is a constant Bad side is updating of
	 * subPlanAncestors, but than has to be done anyway LinkedHashMap
	 * guarantees in order iterator
	 */
	List<String> unpairedTableNames = ts.removeAll();
	unpairedTableNames.addAll(skippedBestTableNames);
	while (!unpairedTableNames.isEmpty()) {
	    final List<String> stillUnprocessed = new ArrayList<String>();
	    // we will try to join all the tables, but some of them cannot be
	    // joined before some other tables
	    // that's why we have while outer loop
	    for (final String unpaired : unpairedTableNames) {
		boolean processed = false;
		for (final Component currentComp : subPlans)
		    if (_pq.getJte().joinExistsBetween(unpaired,
			    ParserUtil.getSourceNameList(currentComp))) {
			final DataSourceComponent unpairedSource = cg
				.generateDataSource(unpaired);
			cg.generateEquiJoin(currentComp, unpairedSource);

			processed = true;
			break;
		    }
		if (!processed)
		    stillUnprocessed.add(unpaired);
	    }
	    unpairedTableNames = stillUnprocessed;
	}

	// third phase: joining Components until there is a single component
	subPlans = cg.getSubPlans();
	while (subPlans.size() > 1) {
	    // this is joining of components having approximately the same
	    // number of ancestors - the same level
	    final Component firstComp = subPlans.get(0);
	    final List<String> firstAncestors = ParserUtil
		    .getSourceNameList(firstComp);
	    for (int i = 1; i < subPlans.size(); i++) {
		final Component otherComp = subPlans.get(i);
		final List<String> otherAncestors = ParserUtil
			.getSourceNameList(otherComp);
		if (_pq.getJte().joinExistsBetween(firstAncestors,
			otherAncestors)) {
		    cg.generateEquiJoin(firstComp, otherComp);
		    break;
		}
	    }
	    // until this point, we change subPlans by locally remove operations
	    // when going to the next level, whesh look over subPlans is taken
	    subPlans = cg.getSubPlans();
	}
	return cg;
    }

    private int getNumHighLevelPairs(int numTables) {
	int highLevelPairs = 0;
	if (numTables == 2)
	    highLevelPairs = 1;
	else if (numTables > 2)
	    highLevelPairs = (numTables % 2 == 0 ? numTables / 2 - 1
		    : numTables / 2);
	return highLevelPairs;
    }

    /*
     * this method returns a list of <ComponentName, whereCompExpression>
     * 
     * @whereCompExpression part of JSQL expression which relates to the
     * corresponding Component
     */
    private Map<String, Expression> getWhereForComponents(Expression whereExpr) {
	final AndVisitor andVisitor = new AndVisitor();
	whereExpr.accept(andVisitor);
	final List<Expression> atomicExprs = andVisitor.getAtomicExprs();
	final List<OrExpression> orExprs = andVisitor.getOrExprs();

	/*
	 * we have to group atomicExpr (conjuctive terms) by ComponentName there
	 * might be mutliple columns from a single DataSourceComponent, and we
	 * want to group them conditions such as R.A + R.B = 10 are possible not
	 * possible to have ColumnReference from multiple tables, because than
	 * it would be join condition
	 */
	final Map<String, Expression> collocatedExprs = new HashMap<String, Expression>();
	ParserUtil.addAndExprsToComps(collocatedExprs, atomicExprs);

	final Map<Set<String>, Expression> collocatedOrs = new HashMap<Set<String>, Expression>();
	ParserUtil.addOrExprsToComps(collocatedOrs, orExprs);

	for (final Map.Entry<Set<String>, Expression> orEntry : collocatedOrs
		.entrySet()) {
	    final List<String> compNames = new ArrayList<String>(
		    orEntry.getKey());
	    final List<Component> compList = ParserUtil.getComponents(
		    compNames, _cg);
	    final Component affectedComponent = HierarchyExtractor
		    .getLCM(compList);

	    final Expression orExpr = orEntry.getValue();
	    ParserUtil.addAndExprToComp(collocatedExprs, orExpr,
		    affectedComponent.getName());
	}

	return collocatedExprs;
    }

    /*************************************************************************************
     * SELECT clause - Final Aggregation
     *************************************************************************************/

    private int processSelectClause(List<SelectItem> selectItems) {
	final IndexSelectItemsVisitor selectVisitor = new IndexSelectItemsVisitor(
		_cg.getQueryBuilder(), _schema, _pq.getTan(), _map);
	for (final SelectItem elem : selectItems)
	    elem.accept(selectVisitor);
	final List<AggregateOperator> aggOps = selectVisitor.getAggOps();
	final List<ValueExpression> groupByVEs = selectVisitor.getGroupByVEs();

	final Component affectedComponent = _cg.getQueryBuilder()
		.getLastComponent();
	attachSelectClause(affectedComponent, aggOps, groupByVEs);
	return (aggOps.isEmpty() ? IndexSelectItemsVisitor.NON_AGG
		: IndexSelectItemsVisitor.AGG);
    }

    /*************************************************************************************
     * WHERE clause - SelectOperator
     *************************************************************************************/

    private void processWhereClause(Expression whereExpr) {
	if (whereExpr == null)
	    return;

	// assinging JSQL expressions to Components
	final Map<String, Expression> whereCompExprPairs = getWhereForComponents(whereExpr);

	// Each component process its own part of JSQL whereExpression
	for (final Map.Entry<String, Expression> whereCompExprPair : whereCompExprPairs
		.entrySet()) {
	    final Component affectedComponent = _cg.getQueryBuilder()
		    .getComponent(whereCompExprPair.getKey());
	    final Expression whereCompExpr = whereCompExprPair.getValue();
	    processWhereForComponent(affectedComponent, whereCompExpr);
	}

    }

    /*
     * whereCompExpression is the part of WHERE clause which refers to
     * affectedComponent This is the only method in this class where
     * IndexWhereVisitor is actually instantiated and invoked
     */
    private void processWhereForComponent(Component affectedComponent,
	    Expression whereCompExpression) {
	final IndexWhereVisitor whereVisitor = new IndexWhereVisitor(
		affectedComponent, _schema, _pq.getTan());
	whereCompExpression.accept(whereVisitor);
	attachWhereClause(affectedComponent, whereVisitor.getSelectOperator());
    }

}