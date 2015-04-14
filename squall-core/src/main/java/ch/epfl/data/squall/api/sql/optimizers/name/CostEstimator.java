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
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import ch.epfl.data.squall.api.sql.estimators.SelingerSelectivityEstimator;
import ch.epfl.data.squall.api.sql.schema.Schema;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.components.OperatorComponent;

/*
 * Responsible for computing selectivities, cardinalities and parallelism for NCG._compCost
 * Instantiates CostParallelismAssigner
 */
public class CostEstimator {
    private final String _queryName;
    private final SQLVisitor _pq;
    private final Schema _schema;

    // needed because NameCompGen sets parallelism for all the components
    private final CostParallelismAssigner _parAssigner;

    private final Map<String, CostParams> _compCost;
    private final SelingerSelectivityEstimator _selEstimator;

    public CostEstimator(String queryName, Schema schema, SQLVisitor pq,
	    Map<String, CostParams> compCost,
	    CostParallelismAssigner parAssigner) {
	_queryName = queryName;
	_pq = pq;
	_schema = schema;
	_compCost = compCost;

	_parAssigner = parAssigner;
	_selEstimator = new SelingerSelectivityEstimator(_queryName, schema,
		_pq.getTan());
    }

    private double computeHashSelectivity(String leftJoinTableSchemaName,
	    String rightJoinTableSchemaName, long leftCardinality,
	    long rightCardinality) {
	final long inputCardinality = leftCardinality + rightCardinality;
	double selectivity;

	if (leftJoinTableSchemaName.equals(rightJoinTableSchemaName))
	    // we treat this as a cross-product on which some selections are
	    // performed
	    // IMPORTANT: selectivity is the output/input rate in the case of
	    // EquiJoin
	    selectivity = (leftCardinality * rightCardinality)
		    / inputCardinality;
	else {
	    double ratio = _schema.getRatio(leftJoinTableSchemaName,
		    rightJoinTableSchemaName);
	    if (ratio < 1)
		// if we are joining bigger and smaller relation, the size of
		// join does not decrease
		// it has to be 1
		ratio = 1;
	    // in case of bushy plans it's proportion of sizes
	    // for lefty plans it's enough to be selectivity of the right parent
	    // component (from compCost)
	    final double rightSelectivity = ((double) rightCardinality)
		    / _schema.getTableSize(rightJoinTableSchemaName);
	    selectivity = (leftCardinality * ratio * rightSelectivity)
		    / inputCardinality;
	}
	return selectivity;
    }

    // ***********HELPER methods***********
    private double computeJoinSelectivity(EquiJoinComponent joinComponent,
	    List<Expression> joinCondition, long leftCardinality,
	    long rightCardinality) {

	final Component[] parents = joinComponent.getParents();
	double selectivity = 1;

	final List<Column> joinColumns = ParserUtil
		.getJSQLColumns(joinCondition);
	final List<String> joinCompNames = ParserUtil
		.getCompNamesFromColumns(joinColumns);

	final List<String> leftJoinTableSchemaNames = getJoinSchemaNames(
		joinCompNames, parents[0]);
	final List<String> rightJoinTableSchemaNames = getJoinSchemaNames(
		joinCompNames, parents[1]);

	if (rightJoinTableSchemaNames.size() > 1)
	    throw new RuntimeException(
		    "Currently, this support only lefty plans!");
	final String rightJoinTableSchemaName = rightJoinTableSchemaNames
		.get(0);

	int i = 0;
	for (final String leftJoinTableSchemaName : leftJoinTableSchemaNames) {
	    double hashSelectivity = computeHashSelectivity(
		    leftJoinTableSchemaName, rightJoinTableSchemaName,
		    leftCardinality, rightCardinality);
	    if (i > 0 && hashSelectivity > 1)
		// having multiple hashSelectivities means that we have
		// AndCondition between them,
		// so they cannot amplify each other.
		hashSelectivity = 1;
	    selectivity *= hashSelectivity;
	    i++;
	}

	return selectivity;
    }

    /*
     * @allJoinCompNames - all the component names from the join condition
     * joinCompNames - all the component names from the join condition
     * corresponding to parent
     */
    private List<String> getJoinSchemaNames(List<String> allJoinCompNames,
	    Component parent) {
	final List<String> ancestors = ParserUtil.getSourceNameList(parent);
	final List<String> joinCompNames = ParserUtil.getIntersection(
		ancestors, allJoinCompNames);

	final List<String> joinSchemaNames = new ArrayList<String>();
	for (final String joinCompName : joinCompNames)
	    joinSchemaNames.add(_pq.getTan().getSchemaName(joinCompName));
	return joinSchemaNames;
    }

    // ***********OPERATORS***********
    public void processWhereCost(Component component, Expression whereCompExpr) {
	if (whereCompExpr != null) {
	    // this is going to change selectivity
	    final String compName = component.getName();
	    final CostParams costParams = _compCost.get(compName);
	    final double previousSelectivity = costParams.getSelectivity();

	    final double selectivity = previousSelectivity
		    * _selEstimator.estimate(whereCompExpr);
	    costParams.setSelectivity(selectivity);
	}
    }

    // ***********SOURCES***********
    // for now only cardinalities
    public void setInputParams(DataSourceComponent source) {
	final String compName = source.getName();
	final String schemaName = _pq.getTan().getSchemaName(compName);

	final CostParams costParams = _compCost.get(compName);
	costParams.setCardinality(_schema.getTableSize(schemaName));
    }

    // ***********EquiJoinComponent***********
    public void setInputParams(EquiJoinComponent joinComponent,
	    List<Expression> joinCondition) {
	final CostParams costParams = _compCost.get(joinComponent.getName());
	final Component[] parents = joinComponent.getParents();

	// ********* set initial (join) selectivity and initial cardinality
	final long leftCardinality = _compCost.get(parents[0].getName())
		.getCardinality();
	final long rightCardinality = _compCost.get(parents[1].getName())
		.getCardinality();

	// compute
	final long inputCardinality = leftCardinality + rightCardinality;
	final double selectivity = computeJoinSelectivity(joinComponent,
		joinCondition, leftCardinality, rightCardinality);

	// setting
	costParams.setCardinality(inputCardinality);
	costParams.setSelectivity(selectivity);
	// *********
    }

    // ***********OperatorComponent***********
    public void setInputParams(OperatorComponent opComp) {
	final CostParams costParams = _compCost.get(opComp.getName());
	final String parentName = opComp.getParents()[0].getName();

	costParams.setCardinality(_compCost.get(parentName).getCardinality());
	costParams.setSelectivity(1);
    }

    // ***********COMMON***********
    // for now only cardinalities
    private void setOutputParams(Component comp) {
	final CostParams costParams = _compCost.get(comp.getName());
	final long currentCardinality = costParams.getCardinality();
	final double selectivity = costParams.getSelectivity();
	final long cardinality = (long) (selectivity * currentCardinality);
	costParams.setCardinality(cardinality);
    }

    public void setOutputParamsAndPar(DataSourceComponent source) {
	setOutputParams(source);

	_parAssigner.setParallelism(source, _compCost);
	_parAssigner.setBatchSize(source, _compCost); // has to go after
	// parallelism
    }

    public void setOutputParamsAndPar(EquiJoinComponent joinComponent) {
	setOutputParams(joinComponent);

	_parAssigner.setParallelism(joinComponent, _compCost);
	_parAssigner.setBatchSize(joinComponent, _compCost); // has to go after
	// setting
	// parallelism
    }

    public void setOutputParamsAndPar(OperatorComponent opComp) {
	setOutputParams(opComp);

	_parAssigner.setParallelism(opComp, _compCost);
	_parAssigner.setBatchSize(opComp, _compCost);
    }

}