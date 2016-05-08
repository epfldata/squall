/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.api.sql.optimizers.name;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Table;
import ch.epfl.data.squall.api.sql.schema.Schema;
import ch.epfl.data.squall.api.sql.util.ImproperParallelismException;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.util.TableAliasName;
import ch.epfl.data.squall.api.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.utilities.SystemParameters;

public class CostParallelismAssigner {
    // If StormDstTupleStorageJoin is used instead of StormDstJoin, I should set 0.3 
    //    (at least according to the Hyracks SQL plan, in which there is no selections on Date)
    private final static double PARALLELISM_PARENTS = 1.0/8; 
    
    /*
     * we need separate class from CostParams, because here we want to order
     * them based on cardinality This class will contain all the parallelism for
     * DataSourceComponents
     */
    private static class OrderedCostParams extends CostParams implements
	    Comparable<OrderedCostParams> {
	/**
		 * 
		 */
	private static final long serialVersionUID = 1L;
	private final String _componentName;

	public OrderedCostParams(String componentName, long cardinality) {
	    _componentName = componentName;
	    setCardinality(cardinality);
	}

	@Override
	public int compareTo(OrderedCostParams t) {
	    final long myCardinality = getCardinality();
	    final long otherCardinality = t.getCardinality();
	    return (new Long(myCardinality)).compareTo(new Long(
		    otherCardinality));
	}

	public String getComponentName() {
	    return _componentName;
	}
    }

    protected final Map _map;
    protected final Schema _schema;

    protected final TableAliasName _tan;
    // computed only once
    protected List<String> _sortedSourceNames;// sorted by increasing

    // cardinalities
    protected Map<String, Integer> _sourcePars;

    public CostParallelismAssigner(Schema schema, TableAliasName tan, Map map) {
	_schema = schema;
	_map = map;
	_tan = tan;
    }

    /*
     * This is done on fake ComponentGenerator, so there is no computeSourcePar
     * parallelism on source components is input variable This method is
     * idempotent, no side effects, can be called multiple times.
     */
    public Map<String, Integer> computeSourcePar(int totalSourcePar) {
	final SQLVisitor pq = ParserUtil.parseQuery(_map);
	final List<Table> tableList = pq.getTableList();
	if (totalSourcePar < tableList.size())
	    throw new RuntimeException(
		    "There is not enought nodes such that all the sources get at least parallelism = 1");
	/*
	 * We need a way to generate parallelism for all the DataSources This
	 * depends on its selectivity/cardinality of each dataSource So we will
	 * generate all the sources witihin a fake sourceCG, and then
	 * proportionally assign parallelism.
	 */
	final NameCompGen sourceCG = new NameCompGen(_schema, _map, this);

	final List<OrderedCostParams> sourceCostParams = new ArrayList<OrderedCostParams>();
	long totalCardinality = 0;
	for (final Table table : tableList) {
	    final DataSourceComponent source = sourceCG
		    .generateDataSource(ParserUtil.getComponentName(table));
	    final String compName = source.getName();
	    final long cardinality = sourceCG.getCostParameters(compName)
		    .getCardinality();
	    totalCardinality += cardinality;
	    sourceCostParams.add(new OrderedCostParams(compName, cardinality));
	}

	/*
	 * Sort by incresing cardinalities, the tables with dblNumNodes = 0 is
	 * assigned 1 and all other proportionally shares the number of nodes We
	 * divide by its output cardinality, because network traffic is the
	 * dominant cost
	 */
	Collections.sort(sourceCostParams);
	int remainingPhysicalNodes = totalSourcePar;
	int remainingLogicalNodes = sourceCostParams.size();
	double remainingCardinality = totalCardinality;
	for (final OrderedCostParams cnc : sourceCostParams) {
	    final long cardinality = cnc.getCardinality();
	    final double ratioNodes = cardinality / remainingCardinality;
	    final double dblNumNodes = ratioNodes * remainingPhysicalNodes;
	    int nodeParallelism = (int) (dblNumNodes + 0.5); // rounding effect
	    // out of the
	    // default
	    // flooring

	    if (nodeParallelism == 0)
		// lower bounds
		nodeParallelism++;
	    if (nodeParallelism + remainingLogicalNodes > remainingPhysicalNodes)
		// upper bound is that all the following sources has at least
		// parallelism = 1
		if (nodeParallelism > 1)
		    nodeParallelism--;
	    if (remainingPhysicalNodes == 0 || nodeParallelism == 0)
		throw new RuntimeException(
			"Not enough nodes, should not be here!");

	    remainingLogicalNodes--;
	    remainingPhysicalNodes -= nodeParallelism;
	    remainingCardinality -= cardinality;

	    // if I am the last guy, I will take all the remaining HW slots
	    if (remainingLogicalNodes == 0)
		nodeParallelism += remainingPhysicalNodes;
	    cnc.setParallelism(nodeParallelism);
	}

	_sortedSourceNames = extractNames(sourceCostParams);
	/*
	 * Now convert it to a Map, so that parallelism for source can be easier
	 * obtained
	 */
	_sourcePars = extractNamesPar(sourceCostParams);
	return _sourcePars;
    }

    /*
     * We take the number of tuples as the upper limit. The real number of
     * distinct hashes may be much smaller, for example when having multiple
     * tuples with the very same hash value. This is rare in practice, in the
     * examples we tried, it occurs only for the final aggregation when last
     * join and final aggregation are not on the same node. In general, it can
     * occur when the join key is not primary key for one of the joined
     * relations, for example when the relationship is n:m and both n,m>0.
     */
    private int estimateDistinctHashes(CostParams leftParentParams,
	    CostParams rightParentParams) {
	/*
	 * TODO: to implement this properly, we need to: - find all the parent
	 * column appearing in joinCondition - column are found by using
	 * ParserUtil.getJSQLColumns(joinCondition) - joinCondition related to
	 * the parents is available in CostOptimizer (obtained
	 * byJoinTableExpr.getExpression(table1, table2)) - check if this column
	 * is the key, or its functional dependency (for example
	 * NATION.NATIONNAME is a functional dependency of NATION.NATIONKEY)
	 */
	final long leftCardinality = leftParentParams.getCardinality();
	final long rightCardinality = rightParentParams.getCardinality();

	long distinctValues = leftCardinality;
	if (distinctValues > rightCardinality)
	    // we return the smaller one
	    distinctValues = rightCardinality;

	if (distinctValues > Integer.MAX_VALUE)
	    return Integer.MAX_VALUE;
	else
	    return (int) distinctValues;

    }

    private int estimateMinParallelism(CostParams leftParentParams,
	    CostParams rightParentParams) {
	final int providedMemory = SystemParameters.getInt(_map,
		"STORAGE_MEMORY_SIZE_MB");
	// inputCardinality tuples need to be stored in memory
	final long inputCardinality = leftParentParams.getCardinality()
		+ rightParentParams.getCardinality();
	final long predictedTotalMemory = (inputCardinality
		* SystemParameters.TUPLE_SIZE_BYTES * SystemParameters.JAVA_OVERHEAD)
		/ SystemParameters.BYTES_IN_MB;
	return (int) (predictedTotalMemory / providedMemory);
    }

    private List<String> extractNames(List<OrderedCostParams> sourceCostParams) {
	final List<String> sortedCompNames = new ArrayList<String>();
	for (final OrderedCostParams cnc : sourceCostParams) {
	    final String compName = cnc.getComponentName();
	    sortedCompNames.add(compName);
	}
	return sortedCompNames;
    }

    private Map<String, Integer> extractNamesPar(
	    List<OrderedCostParams> sourceCostParams) {
	final Map<String, Integer> compParallelism = new HashMap<String, Integer>();
	for (final OrderedCostParams cnc : sourceCostParams) {
	    final String compName = cnc.getComponentName();
	    final int parallelism = cnc.getParallelism();
	    compParallelism.put(compName, parallelism);
	}
	return compParallelism;
    }

    public List<String> getSortedSourceNames() {
	return _sortedSourceNames;
    }

    protected int parallelismFormula(DataSourceComponent source) {
	final String sourceName = source.getName();
	return _sourcePars.get(sourceName);
    }

    protected int parallelismFormula(String compName, CostParams params,
	    CostParams leftParentParams, CostParams rightParentParams) {
	// TODO: this formula does not take into account when joinComponent
	// sends tuples further down
	final double dblParallelism = leftParentParams.getSelectivity()
		* leftParentParams.getParallelism()
		+ rightParentParams.getSelectivity()
		* rightParentParams.getParallelism()
		+ PARALLELISM_PARENTS
		* (leftParentParams.getParallelism() + rightParentParams
			.getParallelism());
	int parallelism = (int) dblParallelism;
	if (parallelism != dblParallelism)
	    // parallelism is ceil of dblParallelism
	    parallelism++;
	return parallelism;
    }

    protected void setBatchSize(DataSourceComponent source,
	    Map<String, CostParams> compCost) {
	// nothing to do
    }

    protected void setBatchSize(EquiJoinComponent joinComponent,
	    Map<String, CostParams> compCost) {
	// nothing to do
    }

    protected void setBatchSize(OperatorComponent operator,
	    Map<String, CostParams> compCost) {
	// nothing to do
    }

    public void setParallelism(DataSourceComponent source,
	    Map<String, CostParams> compCost) {
	if (_sourcePars == null)
	    // if we are here, it was invoked from fake sourceCG, so just return
	    return;

	final String sourceName = source.getName();
	final int parallelism = parallelismFormula(source);
	compCost.get(sourceName).setParallelism(parallelism);
    }

    /*
     * cost-function also idempotent, no changes to `this` changes only compCost
     */
    public void setParallelism(EquiJoinComponent joinComponent,
	    Map<String, CostParams> compCost) {
	final String leftParent = joinComponent.getParents()[0].getName();
	final String rightParent = joinComponent.getParents()[1].getName();

	final String currentCompName = joinComponent.getName();
	final CostParams params = compCost.get(currentCompName);

	final CostParams leftParentParams = compCost.get(leftParent);
	final CostParams rightParentParams = compCost.get(rightParent);

	final int leftParallelism = leftParentParams.getParallelism();
	final int rightParallelism = rightParentParams.getParallelism();

	// compute
	int parallelism = parallelismFormula(currentCompName, params,
		leftParentParams, rightParentParams);

	// lower bound
	final int minParallelism = estimateMinParallelism(leftParentParams,
		rightParentParams);
	if (minParallelism > parallelism)
	    throw new ImproperParallelismException("Component "
		    + currentCompName + " cannot have parallelism LESS than "
		    + minParallelism);

	// upper bound
	final int maxParallelism = estimateDistinctHashes(leftParentParams,
		rightParentParams);
	if (parallelism > maxParallelism)
	    if (leftParallelism == 1 && rightParallelism == 1)
		// if parallelism of both parents is 1, then we should not raise
		// an exception
		// exception serves to force smaller parallelism at sources
		parallelism = maxParallelism;
	    else
		throw new ImproperParallelismException("Component "
			+ currentCompName
			+ " cannot have parallelism MORE than "
			+ maxParallelism);

	// setting
	params.setParallelism(parallelism);

	// we should also check
	// if the sum of all the parallelisms in the subplan
	// is bigger than DIP_NUM_WORKERS (this is set only for PlanRunner).
	// At the time of deciding of parallelism, we are *not* dealing with
	// Storm Config class, but with a plain map.
	// This prevents from reading NUM_WORKERS from Storm Config class.
	// If it works in local mode, it might not work in cluster mode -
	// depending where and when the setting is read.
    }

    /*
     * There could be several algorithms to assign parallelism to opComp: a)
     * square root of parallelism of its parent b) number of distinct groupBy
     * values c) the same as previous level We decided to -if groupBy is a
     * single column, take min(b, c) -otherwise take c
     */
    public void setParallelism(OperatorComponent opComp,
	    Map<String, CostParams> compCost) {
	final Component parent = opComp.getParents()[0];
	final String parentName = parent.getName();
	final int parentPar = compCost.get(parentName).getParallelism();

	int parallelism = parentPar;
	//
	final List<Integer> hashIndexes = parent.getHashIndexes();
	if ((hashIndexes) != null && (hashIndexes.size() == 1)) {
	    final int index = hashIndexes.get(0);
	    final String aliasedColumnName = compCost.get(parentName)
		    .getSchema().getSchema().get(index).getName();
	    final String fullSchemaColumnName = ParserUtil
		    .getFullSchemaColumnName(aliasedColumnName, _tan);

	    try {
		final long distinctValues = _schema
			.getNumDistinctValues(fullSchemaColumnName);
		if (distinctValues < parallelism)
		    parallelism = (int) distinctValues;
	    } catch (final RuntimeException ex) {
	    }
	}

	// cannot be less than 1
	if (parallelism < 1)
	    parallelism = 1;

	// setting
	final String currentComp = opComp.getName();
	compCost.get(currentComp).setParallelism(parallelism);
    }
}