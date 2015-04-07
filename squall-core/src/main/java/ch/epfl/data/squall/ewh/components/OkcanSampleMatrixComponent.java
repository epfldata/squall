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


package ch.epfl.data.squall.ewh.components;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.ewh.storm_components.OkcanSampleMatrixBolt;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.Type;

public class OkcanSampleMatrixComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger.getLogger(EquiJoinComponent.class);

	private final Component _firstParent;
	private final Component _secondParent;
	private String _componentName;

	private int _numOfLastJoiners;
	private ComparisonPredicate _comparison;
	private NumericType _wrapper;
	private int _firstNumOfBuckets, _secondNumOfBuckets;

	public OkcanSampleMatrixComponent(Component firstParent,
			Component secondParent, NumericType keyType,
			ComparisonPredicate comparison, int numOfLastJoiners,
			int firstNumOfBuckets, int secondNumOfBuckets) {
		_firstParent = firstParent;
		_firstParent.setChild(this);
		_secondParent = secondParent;
		_secondParent.setChild(this);
		_componentName = "OKCAN_SAMPLE_MATRIX";

		_numOfLastJoiners = numOfLastJoiners;
		_comparison = comparison;
		_wrapper = keyType;
		_firstNumOfBuckets = firstNumOfBuckets;
		_secondNumOfBuckets = secondNumOfBuckets;
	}

	// below is not used
	@Override
	public OkcanSampleMatrixComponent add(Operator operator) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Component)
			return _componentName.equals(((Component) obj).getName());
		else
			return false;
	}

	@Override
	public List<DataSourceComponent> getAncestorDataSources() {
		final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
		for (final Component parent : getParents())
			list.addAll(parent.getAncestorDataSources());
		return list;
	}

	@Override
	public long getBatchOutputMillis() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public ChainOperator getChainOperator() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public Component getChild() {
		return null;
	}

	// from StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public List<String> getFullHashList() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public List<ValueExpression> getHashExpressions() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public List<Integer> getHashIndexes() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public String getInfoID() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public String getName() {
		return _componentName;
	}

	@Override
	public Component[] getParents() {
		return new Component[] { _firstParent, _secondParent };
	}

	@Override
	public boolean getPrintOut() {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 37 * hash
				+ (_componentName != null ? _componentName.hashCode() : 0);
		return hash;
	}

	@Override
	public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
			List<String> allCompNames, Config conf, int hierarchyPosition) {

		new OkcanSampleMatrixBolt(_firstParent, _secondParent, _componentName,
				_numOfLastJoiners, _wrapper, _comparison, _firstNumOfBuckets,
				_secondNumOfBuckets, allCompNames, builder, killer, conf);
	}

	@Override
	public OkcanSampleMatrixComponent setBatchOutputMillis(long millis) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public void setChild(Component child) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public Component setContentSensitiveThetaJoinWrapper(Type wrapper) {
		throw new RuntimeException("Should not be here!");
	}

	// list of distinct keys, used for direct stream grouping and load-balancing
	// ()
	@Override
	public OkcanSampleMatrixComponent setFullHashList(List<String> fullHashList) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public OkcanSampleMatrixComponent setHashExpressions(
			List<ValueExpression> hashExpressions) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public Component setInterComp(InterchangingComponent inter) {
		throw new RuntimeException(
				"EquiJoin component does not support setInterComp");
	}

	@Override
	public OkcanSampleMatrixComponent setJoinPredicate(Predicate predicate) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public OkcanSampleMatrixComponent setOutputPartKey(int... hashIndexes) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public OkcanSampleMatrixComponent setOutputPartKey(List<Integer> hashIndexes) {
		throw new RuntimeException("Should not be here!");
	}

	@Override
	public OkcanSampleMatrixComponent setPrintOut(boolean printOut) {
		throw new RuntimeException("Should not be here!");
	}

	// Out of the second storage (join of R tuple with S relation)
	public OkcanSampleMatrixComponent setSecondPreAggProj(
			ProjectOperator secondPreAggProj) {
		throw new RuntimeException("Should not be here!");
	}

	public OkcanSampleMatrixComponent setSecondPreAggStorage(
			AggregationStorage secondPreAggStorage) {
		throw new RuntimeException("Should not be here!");
	}
}