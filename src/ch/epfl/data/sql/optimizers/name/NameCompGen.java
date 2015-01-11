package ch.epfl.data.sql.optimizers.name;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.components.EquiJoinComponent;
import ch.epfl.data.plan_runner.components.OperatorComponent;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import ch.epfl.data.plan_runner.utilities.DeepCopy;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.sql.optimizers.CompGen;
import ch.epfl.data.sql.schema.Schema;
import ch.epfl.data.sql.util.HierarchyExtractor;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.util.TupleSchema;
import ch.epfl.data.sql.visitors.jsql.AndVisitor;
import ch.epfl.data.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.sql.visitors.squall.NameJoinHashVisitor;
import ch.epfl.data.sql.visitors.squall.NameSelectItemsVisitor;
import ch.epfl.data.sql.visitors.squall.NameWhereVisitor;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.statement.select.SelectItem;

/*
 * It is necessary that this class operates with Tables,
 *   since we don't want multiple CG sharing the same copy of DataSourceComponent.
 */
public class NameCompGen implements CompGen {
	private final SQLVisitor _pq;

	private final Map _map;
	private final Schema _schema;
	private final String _dataPath;
	private final String _extension;
	private final String _queryName;

	private QueryBuilder _queryBuilder = new QueryBuilder();

	// compName, CostParams for all the components from _queryPlan
	private Map<String, CostParams> _compCost = new HashMap<String, CostParams>();

	private CostEstimator _costEst;
	private CostParallelismAssigner _parAssigner;

	// used for SelectOperator (only from WHERE clause)
	private final Map<String, Expression> _compNamesAndExprs = new HashMap<String, Expression>();
	private final Map<Set<String>, Expression> _compNamesOrExprs = new HashMap<Set<String>, Expression>();

	// used for ProjectOperator (both from SELECT and WHERE clauses)
	private final ProjGlobalCollect _globalCollect;

	// we don't use it, because we have always to do deepCopy because of
	// translateExpr
	// public NameCompGen(Schema schema,
	// SQLVisitor pq,
	// Map map,
	// CostParallelismAssigner parAssigner,
	// Map<String, Expression> compNamesAndExprs,
	// Map<Set<String>, Expression> compNamesOrExprs,
	// ProjGlobalCollect globalCollect){
	// _pq = pq;
	// _map = map;
	// _schema = schema;
	//
	// _dataPath = SystemParameters.getString(map, "DIP_DATA_PATH");
	// _extension = SystemParameters.getString(map, "DIP_EXTENSION");
	//
	// if(parAssigner != null){
	// _parAssigner = parAssigner;
	// _costEst = new CostEstimator(schema, pq, _compCost, parAssigner);
	// }
	//
	// _compNamesAndExprs = compNamesAndExprs;
	// _compNamesOrExprs = compNamesOrExprs;
	//
	// _globalCollect = globalCollect;
	// }

	// CPA initialized in NameCompGenFactory
	public NameCompGen(Schema schema, Map map, CostParallelismAssigner parAssigner) {

		_schema = schema;
		_map = map;

		_pq = ParserUtil.parseQuery(map);
		_dataPath = SystemParameters.getString(map, "DIP_DATA_PATH");
		_extension = SystemParameters.getString(map, "DIP_EXTENSION");
		_queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");

		if (parAssigner != null) {
			_parAssigner = parAssigner;
			_costEst = new CostEstimator(_queryName, schema, _pq, _compCost, parAssigner);
		}

		// initializes _compNamesAndExprs and _compNamesOrExprs
		initWhereClause(_pq.getWhereExpr());

		_globalCollect = new ProjGlobalCollect(_pq.getSelectItems(), _pq.getWhereExpr());
		_globalCollect.process();
	}

	private void addHash(Component component, List<ValueExpression> hashExpressions) {
		// if joinCondition is a R.A + 5 = S.A, and inputTupleSchema is
		// "R.A + 5", HashExpression is a ColumnReference(0)
		if (ParserUtil.isAllColumnRefs(hashExpressions)) {
			// all the join conditions are represented through columns, no
			// ValueExpression
			// guaranteed that both joined components will have joined columns
			// visited in the same order
			// i.e R.A=S.A and R.B=S.B, the columns are (R.A, R.B), (S.A, S.B),
			// respectively
			final List<Integer> hashIndexes = ParserUtil.extractColumnIndexes(hashExpressions);

			// hash indexes in join condition
			component.setOutputPartKey(hashIndexes);
		} else
			// hash expressions in join condition
			component.setHashExpressions(hashExpressions);
	}

	/*************************************************************************************
	 * HASH
	 *************************************************************************************/

	// set hash for this component, knowing its position in the query plan.
	// Conditions are related only to parents of join,
	// but we have to filter who belongs to my branch in NameJoinHashVisitor.
	// We don't want to hash on something which will be used to join with same
	// later component in the hierarchy.
	private void addJoinHash(Component component, List<Expression> joinCondition) {
		final TupleSchema tupleSchema = _compCost.get(component.getName()).getSchema();
		final NameJoinHashVisitor joinOn = new NameJoinHashVisitor(tupleSchema, component);
		for (final Expression exp : joinCondition)
			exp.accept(joinOn);
		final List<ValueExpression> hashExpressions = joinOn.getExpressions();

		addHash(component, hashExpressions);
	}

	/*************************************************************************************
	 * Project operator
	 *************************************************************************************/
	private void addProjectOperator(Component component) {
		final String compName = component.getName();
		final TupleSchema inputTupleSchema = _compCost.get(compName).getSchema();
		final ProjSchemaCreator psc = new ProjSchemaCreator(_globalCollect, inputTupleSchema,
				component, _pq, _schema);
		psc.create();

		final TupleSchema outputTupleSchema = psc.getOutputSchema();

		if (!ParserUtil.isSameSchema(inputTupleSchema, outputTupleSchema)) {
			// no need to add projectOperator unless it changes something
			attachProjectOperator(component, psc.getProjectOperator());
			processProjectCost(component, outputTupleSchema);
		}
	}

	/*************************************************************************************
	 * WHERE clause - SelectOperator
	 *************************************************************************************/
	private void addSelectOperator(Component component) {
		final Expression whereCompExpr = createWhereForComponent(component);

		processWhereForComponent(component, whereCompExpr);
		if (_costEst != null)
			_costEst.processWhereCost(component, whereCompExpr);
	}

	private Expression appendAnd(Expression fullExpr, Expression atomicExpr) {
		if (atomicExpr != null)
			if (fullExpr != null)
				// appending to previous expressions
				fullExpr = new AndExpression(fullExpr, atomicExpr);
			else
				// this is the first expression for this component
				fullExpr = atomicExpr;
		return fullExpr;
	}

	private Expression appendOr(Expression fullExpr, Expression atomicExpr) {
		if (atomicExpr != null)
			if (fullExpr != null)
				// appending to previous expressions
				fullExpr = new OrExpression(fullExpr, atomicExpr);
			else
				// this is the first expression for this component
				fullExpr = atomicExpr;
		return fullExpr;
	}

	private void attachProjectOperator(Component component, ProjectOperator project) {
		component.add(project);
	}

	private void attachSelectClauseOnLastJoin(Component lastComponent,
			NameSelectItemsVisitor selectVisitor) {
		final List<AggregateOperator> aggOps = selectVisitor.getAggOps();
		ProjectOperator project = null;
		if (!(selectVisitor.getGroupByVEs() == null || selectVisitor.getGroupByVEs().isEmpty()))
			project = new ProjectOperator(selectVisitor.getGroupByVEs());

		if (aggOps.isEmpty()) {
			if (project != null)
				lastComponent.add(project);
		} else if (aggOps.size() == 1) {
			// all the others are group by
			final AggregateOperator firstAgg = aggOps.get(0);
			if (project != null)
				firstAgg.setGroupByProjection(project);

			/*
			 * Avg result cannot be aggregated over multiple nodes. Solution is
			 * one of the following: a) the output of average is keeped in a
			 * form (Sum, Count) and then a user is responsible to aggregate it
			 * over nodes b) if NameTranslator.isSuperset for last join keys and
			 * GroupBy is not fullfilled create new level node with aggregation
			 * as the only operation To be akin to Sum and Count aggregates, we
			 * opted for a)
			 */
			if (firstAgg.getDistinct() == null)
				lastComponent.add(firstAgg);
			else
				// in general groupByVEs is not a ColumnReference (it can be an
				// addition, for example).
				// ProjectOperator is not obliged to create schema which fully
				// fits in what FinalAggregation wants
				addHash(lastComponent, selectVisitor.getGroupByVEs());
		} else
			throw new RuntimeException("For now only one aggregate function supported!");
	}

	private void attachWhereClause(Component affectedComponent, SelectOperator select) {
		affectedComponent.add(select);
	}

	private DataSourceComponent createAddDataSource(String tableCompName) {
		final String tableSchemaName = _pq.getTan().getSchemaName(tableCompName);
		final String sourceFile = tableSchemaName.toLowerCase();

		final DataSourceComponent relation = new DataSourceComponent(tableCompName, _dataPath
				+ sourceFile + _extension);
		_queryBuilder.add(relation);
		return relation;
	}

	private EquiJoinComponent createAndAddEquiJoin(Component left, Component right) {
		final EquiJoinComponent joinComponent = new EquiJoinComponent(left, right);
		_queryBuilder.add(joinComponent);

		return joinComponent;
	}

	private OperatorComponent createAndAddOperatorComp(Component lastComponent) {
		final OperatorComponent opComp = new OperatorComponent(lastComponent,
				ParserUtil.generateUniqueName("OPERATOR"));
		_queryBuilder.add(opComp);

		return opComp;
	}

	/*
	 * Setting schema for DataSourceComponent
	 */
	private void createCompCost(DataSourceComponent source) {
		final String compName = source.getName();
		final String schemaName = _pq.getTan().getSchemaName(compName);
		final CostParams costParams = new CostParams();

		// schema is consisted of TableAlias.columnName
		costParams.setSchema(ParserUtil.createAliasedSchema(_schema.getTableSchema(schemaName),
				compName));

		_compCost.put(compName, costParams);
	}

	/*
	 * This can estimate selectivity/cardinality of a join between between any
	 * two components but with a restriction - rightParent has only one
	 * component mentioned in joinCondition. If connection between any
	 * components is allowed, we have to find a way combining multiple distinct
	 * selectivities (for example having a component R-S and T-V, how to combine
	 * R.A=T.A and S.B=V.B?) This method is based on usual way to join tables -
	 * on their appropriate keys. It works for cyclic queries as well (TPCH5 is
	 * an example).
	 */
	private void createCompCost(EquiJoinComponent joinComponent) {
		// create schema and selectivity wrt leftParent
		final String compName = joinComponent.getName();
		final CostParams costParams = new CostParams();

		// *********set schema
		final TupleSchema schema = ParserUtil.joinSchema(joinComponent.getParents(), _compCost);
		costParams.setSchema(schema);

		_compCost.put(compName, costParams);
	}

	private void createCompCost(OperatorComponent opComp) {
		final String compName = opComp.getName();
		final CostParams costParams = new CostParams();

		// *********set schema
		final TupleSchema schema = _compCost.get(opComp.getParents()[0].getName()).getSchema();
		costParams.setSchema(schema);

		_compCost.put(compName, costParams);
	}

	/*
	 * Merging atomicExpr and orExpressions corresponding to this component
	 */
	private Expression createWhereForComponent(Component component) {
		Expression expr = _compNamesAndExprs.get(component.getName());

		for (final Map.Entry<Set<String>, Expression> orEntry : _compNamesOrExprs.entrySet()) {
			final Set<String> orCompNames = orEntry.getKey();

			// TODO-PRIO: the full solution would be that OrExpressions are
			// split into subexpressions
			// which might be executed on their LCM
			// Not implemented because it's quite rare - only TPCH7
			// Even in TPCH7 there is no need for multiple LCM.
			// TODO-PRIO: selectivityEstimation for pushing OR need to be
			// improved
			final Expression orExpr = orEntry.getValue();
			if (HierarchyExtractor.isLCM(component, orCompNames))
				expr = appendAnd(expr, orExpr);
			else if (component instanceof DataSourceComponent) {
				final DataSourceComponent source = (DataSourceComponent) component;
				final Expression addedExpr = getMineSubset(source, orExpr);
				expr = appendAnd(expr, addedExpr);
			}
		}
		return expr;
	}

	/*
	 * Used in CostOptimizer when different plans are possible from the same
	 * subplan main reason is translateExpr method - synonyms in NameTranslator
	 */
	public NameCompGen deepCopy() {
		// map, schema, dataPath, dataExt are shared because they are constants
		// all the time
		// parAssigner is computed once in NameCompGenFactory and then can be
		// shared

		// pq, globalProject, compNamesAndExprs, compNamesOrExprs are created
		// from scratch in the constructor
		// ideally, this should be deep-copied, because it can be changed due to
		// NameTranslator.synonims
		// not possible because JSQL is not serializable
		// but this is only matter of performance
		final NameCompGen copy = new NameCompGen(_schema, _map, _parAssigner);

		// the rest needs to be explicitly deep-copied
		copy._compCost = (Map<String, CostParams>) DeepCopy.copy(_compCost);

		// _compCost from Estimator and from NCG has to be the same reference
		copy._costEst = new CostEstimator(_queryName, copy._schema, copy._pq, copy._compCost,
				copy._parAssigner);

		copy._queryBuilder = (QueryBuilder) DeepCopy.copy(_queryBuilder);
		return copy;
	}

	/*
	 * adding a DataSourceComponent to the list of components
	 */
	@Override
	public DataSourceComponent generateDataSource(String tableCompName) {
		final DataSourceComponent source = createAddDataSource(tableCompName);

		createCompCost(source);
		if (_costEst != null)
			_costEst.setInputParams(source);

		// operators
		addSelectOperator(source);
		addProjectOperator(source);

		// For single-dataSource plans (such as TPCH6)
		NameSelectItemsVisitor nsiv = null;
		if (ParserUtil.isFinalComponent(source, _pq)) {
			// final component in terms of joins
			nsiv = getFinalSelectVisitor(source);
			attachSelectClauseOnLastJoin(source, nsiv);
		}

		if (_costEst != null)
			_costEst.setOutputParamsAndPar(source);

		// we have to create newComponent after processing statistics of the
		// joinComponent
		if (ParserUtil.isFinalComponent(source, _pq))
			generateOperatorComp(source, nsiv);

		return source;
	}

	/*
	 * Join between two components List<Expression> is a set of join conditions
	 * between two components.
	 */
	@Override
	public EquiJoinComponent generateEquiJoin(Component left, Component right) {
		final EquiJoinComponent joinComponent = createAndAddEquiJoin(left, right);

		// compute join condition
		final List<Expression> joinCondition = ParserUtil.getJoinCondition(_pq, left, right);
		if (joinCondition == null)
			throw new RuntimeException("There is no join conditition between components "
					+ left.getName() + " and " + right.getName());

		// set hashes for two parents, has to be before createCompCost
		addJoinHash(left, joinCondition);
		addJoinHash(right, joinCondition);

		createCompCost(joinComponent);
		if (_costEst != null)
			_costEst.setInputParams(joinComponent, joinCondition);

		// operators
		addSelectOperator(joinComponent);

		// TODO when single last component: decomment when NSIV.visit(Column) is
		// fixed
		// - issue in TPCH9
		// if(!ParserUtil.isFinalJoin(joinComponent, _pq)){
		addProjectOperator(joinComponent);
		// assume no operators between projection and final aggregation
		// final aggregation is able to do projection in GroupByProjection
		// }

		NameSelectItemsVisitor nsiv = null;
		if (ParserUtil.isFinalComponent(joinComponent, _pq)) {
			// final component in terms of joins
			nsiv = getFinalSelectVisitor(joinComponent);
			attachSelectClauseOnLastJoin(joinComponent, nsiv);
		}

		if (_costEst != null)
			_costEst.setOutputParamsAndPar(joinComponent);

		// we have to create newComponent after processing statistics of the
		// joinComponent
		if (ParserUtil.isFinalComponent(joinComponent, _pq))
			generateOperatorComp(joinComponent, nsiv);

		return joinComponent;
	}

	private OperatorComponent generateOperatorComp(Component lastComponent,
			NameSelectItemsVisitor selectVisitor) {
		final List<AggregateOperator> aggOps = selectVisitor.getAggOps();
		if (aggOps.size() != 1)
			return null;
		OperatorComponent opComp = null;

		// projectOperator is already set to firstAgg in attachLastJoin method
		// if we decide to do construct new NSIV, then projectOperator has to be
		// set as well
		final AggregateOperator firstAgg = aggOps.get(0);

		// Setting new level of components is only necessary for distinct in
		// aggregates
		if (firstAgg.getDistinct() != null) {
			opComp = createAndAddOperatorComp(lastComponent);

			createCompCost(opComp);
			if (_costEst != null)
				_costEst.setInputParams(opComp);

			// we can use the same firstAgg, because we no tupleSchema change
			// occurred after LAST_COMPONENT:FinalAgg and NEW_COMPONENT:FinalAgg
			// Namely, NEW_COMPONENT has only FinalAgg operator
			opComp.add(firstAgg);

			if (_costEst != null)
				_costEst.setOutputParamsAndPar(opComp);
		}

		return opComp;
	}

	public Map<String, CostParams> getCompCost() {
		return _compCost;
	}

	public CostParams getCostParameters(String componentName) {
		return _compCost.get(componentName);
	}

	/*************************************************************************************
	 * SELECT clause - Final aggregation
	 *************************************************************************************/
	private NameSelectItemsVisitor getFinalSelectVisitor(Component lastComponent) {
		final TupleSchema tupleSchema = _compCost.get(lastComponent.getName()).getSchema();
		final NameSelectItemsVisitor selectVisitor = new NameSelectItemsVisitor(tupleSchema, _map,
				lastComponent);
		for (final SelectItem elem : _pq.getSelectItems())
			elem.accept(selectVisitor);
		return selectVisitor;
	}

	/*
	 * get a list of WhereExpressions (connected by OR) belonging to source For
	 * example (N1.NATION = FRANCE AND N2.NATION = GERMANY) OR (N1.NATION =
	 * GERMANY AND N2.NATION = FRANCE) returns N1.NATION = FRANCE OR N1.NATION =
	 * GERMANY
	 */
	public Expression getMineSubset(DataSourceComponent source, Expression expr) {
		final List<String> compNames = ParserUtil.getCompNamesFromColumns(ParserUtil
				.getJSQLColumns(expr));

		boolean mine = true;
		for (final String compName : compNames)
			if (!compName.equals(source.getName())) {
				mine = false;
				break;
			}

		if (mine)
			return expr;

		Expression result = null;
		if (expr instanceof OrExpression || expr instanceof AndExpression) {
			final BinaryExpression be = (BinaryExpression) expr;
			result = appendOr(result, getMineSubset(source, be.getLeftExpression()));
			result = appendOr(result, getMineSubset(source, be.getRightExpression()));
		} else if (expr instanceof Parenthesis) {
			final Parenthesis prnth = (Parenthesis) expr;
			result = getMineSubset(source, prnth.getExpression());
		}

		// whatever is not fully recognized (all the compNames = source), and is
		// not And or Or, returns null
		return result;
	}

	@Override
	public QueryBuilder getQueryBuilder() {
		return _queryBuilder;
	}

	@Override
	public List<Component> getSubPlans() {
		throw new RuntimeException("Should not be invoked for lefty plans!");
	}

	private void initWhereClause(Expression whereExpr) {
		if (whereExpr == null)
			return;

		final AndVisitor andVisitor = new AndVisitor();
		whereExpr.accept(andVisitor);
		final List<Expression> atomicAndExprs = andVisitor.getAtomicExprs();
		final List<OrExpression> orExprs = andVisitor.getOrExprs();

		/*
		 * we have to group atomicExpr (conjunctive terms) by ComponentName
		 * there might be multiple columns from a single DataSourceComponent,
		 * and we want to group them
		 * conditions such as R.A + R.B = 10 are possible not possible to have
		 * ColumnReference from multiple tables, because than it would be join
		 * condition
		 */
		ParserUtil.addAndExprsToComps(_compNamesAndExprs, atomicAndExprs);
		ParserUtil.addOrExprsToComps(_compNamesOrExprs, orExprs);
	}

	private void processProjectCost(Component component, TupleSchema outputTupleSchema) {
		// only schema is changed
		final String compName = component.getName();
		_compCost.get(compName).setSchema(outputTupleSchema);
	}

	/*
	 * whereCompExpression is the part of WHERE clause which refers to
	 * affectedComponent This is the only method in this class where
	 * IndexWhereVisitor is actually instantiated and invoked
	 * SelectOperator is able to deal with ValueExpressions (and not only with
	 * ColumnReferences), but here we recognize JSQL expressions here which can
	 * be built of inputTupleSchema (constants included)
	 */
	private void processWhereForComponent(Component affectedComponent, Expression whereCompExpr) {
		if (whereCompExpr != null) {
			// first get the current schema of the component
			final TupleSchema tupleSchema = _compCost.get(affectedComponent.getName()).getSchema();
			final NameWhereVisitor whereVisitor = new NameWhereVisitor(tupleSchema,
					affectedComponent);
			whereCompExpr.accept(whereVisitor);
			attachWhereClause(affectedComponent, whereVisitor.getSelectOperator());
		}
	}

}
