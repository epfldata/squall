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


package ch.epfl.data.squall.components;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormInterchangingDataSource;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.Type;

public class InterchangingDataSourceComponent implements Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger
			.getLogger(InterchangingDataSourceComponent.class);

	private final String _componentName;
	private final String _inputPathRel1, _inputPathRel2;

	private long _batchOutputMillis;

	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;

	private StormInterchangingDataSource _dataSource;

	private final ChainOperator _chainRel1 = new ChainOperator();
	private final ChainOperator _chainRel2 = new ChainOperator();

	private boolean _printOut;
	private boolean _printOutSet; // whether printOut condition is already set

	private Component _child;

	private final int _multFactor;

	public InterchangingDataSourceComponent(String componentName,
			String inputPath1, String inputPath2, int multfactor) {
		_componentName = componentName;
		_inputPathRel1 = inputPath1;
		_inputPathRel2 = inputPath2;
		_multFactor = multfactor;
	}

	@Override
	public Component add(Operator operator) {
		return null;
	}

	public InterchangingDataSourceComponent addOperatorRel1(Operator operator) {
		_chainRel1.addOperator(operator);
		return this;
	}

	public InterchangingDataSourceComponent addOperatorRel2(Operator operator) {
		_chainRel2.addOperator(operator);
		return this;
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
		// List<InterchangingDataSourceComponent> list = new
		// ArrayList<InterchangingDataSourceComponent>();
		// list.add(this);
		// return list;
		return null;
	}

	@Override
	public long getBatchOutputMillis() {
		return _batchOutputMillis;
	}

	// IGNORE
	@Override
	public ChainOperator getChainOperator() {
		return null;
	}

	public ChainOperator getChainOperatorRel1() {
		return _chainRel1;
	}

	public ChainOperator getChainOperatorRel2() {
		return _chainRel2;
	}

	@Override
	public Component getChild() {
		return _child;
	}

	// from StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		return _dataSource.getEmitterIDs();
	}

	@Override
	public List<String> getFullHashList() {
		throw new RuntimeException(
				"This method should not be invoked for DataSourceComponent!");
	}

	@Override
	public List<ValueExpression> getHashExpressions() {
		return _hashExpressions;
	}

	@Override
	public List<Integer> getHashIndexes() {
		return _hashIndexes;
	}

	@Override
	public String getInfoID() {
		return _dataSource.getInfoID() + "\n";
	}

	@Override
	public String getName() {
		return _componentName;
	}

	@Override
	public Component[] getParents() {
		return null;
	}

	@Override
	public boolean getPrintOut() {
		return _printOut;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 59 * hash
				+ (_componentName != null ? _componentName.hashCode() : 0);
		return hash;
	}

	@Override
	public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
			List<String> allCompNames, Config conf, int hierarchyPosition) {

		// by default print out for the last component
		// for other conditions, can be set via setPrintOut
		if (hierarchyPosition == StormComponent.FINAL_COMPONENT
				&& !_printOutSet)
			setPrintOut(true);

		_dataSource = new StormInterchangingDataSource(this, allCompNames,
				_multFactor, _inputPathRel1, _inputPathRel2, hierarchyPosition,
				builder, killer, conf);
	}

	@Override
	public InterchangingDataSourceComponent setBatchOutputMillis(long millis) {
		throw new RuntimeException(
				"Setting batch mode is not allowed for DataSourceComponents!");
		// _batchOutputMillis = millis;
		// return this;
	}

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	@Override
	public Component setContentSensitiveThetaJoinWrapper(Type wrapper) {
		return this;
	}

	@Override
	public InterchangingDataSourceComponent setFullHashList(
			List<String> fullHashList) {
		throw new RuntimeException(
				"This method should not be invoked for DataSourceComponent!");
	}

	@Override
	public InterchangingDataSourceComponent setHashExpressions(
			List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public Component setInterComp(InterchangingComponent inter) {
		throw new RuntimeException(
				"InterchangingDatasource component does not support setInterComp");
	}

	@Override
	public Component setJoinPredicate(Predicate joinPredicate) {
		throw new RuntimeException(
				"InterchangingDatasource component does not support Join Predicates");
	}

	@Override
	public InterchangingDataSourceComponent setOutputPartKey(int... hashIndexes) {
		return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
	}

	@Override
	public InterchangingDataSourceComponent setOutputPartKey(
			List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}

	@Override
	public InterchangingDataSourceComponent setPrintOut(boolean printOut) {
		_printOutSet = true;
		_printOut = printOut;
		return this;
	}
}