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


package ch.epfl.data.squall.components.theta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.JoinerComponent;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormBoltComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.storm_components.theta.StormThetaJoin;
import ch.epfl.data.squall.storm_components.theta.StormThetaJoinBDB;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class ThetaJoinStaticComponent extends JoinerComponent implements
		Component {
	private static final long serialVersionUID = 1L;
	private static Logger LOG = Logger
			.getLogger(ThetaJoinStaticComponent.class);
	private final Component _firstParent;
	private final Component _secondParent;
	private Component _child;
	private final String _componentName;
	private long _batchOutputMillis;
	private List<Integer> _hashIndexes;
	private List<ValueExpression> _hashExpressions;
	private StormBoltComponent _joiner;
	private final ChainOperator _chain = new ChainOperator();
	private boolean _printOut;
	private boolean _printOutSet; // whether printOut was already set
	private boolean _isContentSensitive;
	private Predicate _joinPredicate;
	private InterchangingComponent _interComp = null;
	private TypeConversion _contentSensitiveThetaJoinWrapper = null;

	// equi-weight histogram
	private boolean _isPartitioner;

	public ThetaJoinStaticComponent(Component firstParent,
			Component secondParent, boolean isContentSensitive) {
		_firstParent = firstParent;
		_firstParent.setChild(this);
		_secondParent = secondParent;
		_secondParent.setChild(this);
		_componentName = firstParent.getName() + "_" + secondParent.getName();
		_isContentSensitive = isContentSensitive;
	}

	@Override
	public ThetaJoinStaticComponent add(Operator operator) {
		_chain.addOperator(operator);
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
		final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
		for (final Component parent : getParents())
			list.addAll(parent.getAncestorDataSources());
		return list;
	}

	@Override
	public long getBatchOutputMillis() {
		return _batchOutputMillis;
	}

	@Override
	public ChainOperator getChainOperator() {
		return _chain;
	}

	@Override
	public Component getChild() {
		return _child;
	}

	// from StormEmitter interface
	@Override
	public String[] getEmitterIDs() {
		return _joiner.getEmitterIDs();
	}

	@Override
	public List<String> getFullHashList() {
		throw new RuntimeException(
				"Load balancing for Theta join is done inherently!");
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
		return _joiner.getInfoID();
	}

	public Predicate getJoinPredicate() {
		return _joinPredicate;
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
		return _printOut;
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

		// by default print out for the last component
		// for other conditions, can be set via setPrintOut
		if (hierarchyPosition == StormComponent.FINAL_COMPONENT
				&& !_printOutSet)
			setPrintOut(true);

		MyUtilities.checkBatchOutput(_batchOutputMillis,
				_chain.getAggregation(), conf);

		boolean isBDB = MyUtilities.isBDB(conf);
		if (isBDB && _joinPredicate == null) {
			throw new RuntimeException(
					"Please provide _joinPredicate if you want to run BDB!");
		}

		if (isBDB && (hierarchyPosition == StormComponent.FINAL_COMPONENT)) {
			_joiner = new StormThetaJoinBDB(_firstParent, _secondParent, this,
					allCompNames, _joinPredicate, hierarchyPosition, builder,
					killer, conf, _interComp);

		} else {
			_joiner = new StormThetaJoin(_firstParent, _secondParent, this,
					allCompNames, _joinPredicate, _isPartitioner,
					hierarchyPosition, builder, killer, conf, _interComp,
					_isContentSensitive, _contentSensitiveThetaJoinWrapper);
		}
		if (_windowSize > 0 || _tumblingWindowSize > 0)
			_joiner.setWindowSemantics(_windowSize, _tumblingWindowSize);
	}

	@Override
	public ThetaJoinStaticComponent setBatchOutputMillis(long millis) {
		_batchOutputMillis = millis;
		return this;
	}

	@Override
	public void setChild(Component child) {
		_child = child;
	}

	@Override
	public ThetaJoinStaticComponent setContentSensitiveThetaJoinWrapper(
			TypeConversion wrapper) {
		_contentSensitiveThetaJoinWrapper = wrapper;
		return this;
	}

	// list of distinct keys, used for direct stream grouping and load-balancing
	// ()
	@Override
	public ThetaJoinStaticComponent setFullHashList(List<String> fullHashList) {
		throw new RuntimeException(
				"Load balancing for Theta join is done inherently!");
	}

	@Override
	public ThetaJoinStaticComponent setHashExpressions(
			List<ValueExpression> hashExpressions) {
		_hashExpressions = hashExpressions;
		return this;
	}

	@Override
	public ThetaJoinStaticComponent setInterComp(InterchangingComponent inter) {
		_interComp = inter;
		return this;
	}

	@Override
	public ThetaJoinStaticComponent setJoinPredicate(Predicate joinPredicate) {
		_joinPredicate = joinPredicate;
		return this;
	}

	@Override
	public ThetaJoinStaticComponent setOutputPartKey(int... hashIndexes) {
		return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
	}

	@Override
	public ThetaJoinStaticComponent setOutputPartKey(List<Integer> hashIndexes) {
		_hashIndexes = hashIndexes;
		return this;
	}

	public ThetaJoinStaticComponent setPartitioner(boolean isPartitioner) {
		_isPartitioner = isPartitioner;
		return this;
	}

	@Override
	public ThetaJoinStaticComponent setPrintOut(boolean printOut) {
		_printOutSet = true;
		_printOut = printOut;
		return this;
	}

	@Override
	public Component setSlidingWindow(int windowRange) {
		WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
		_windowSize = windowRange * 1000; // Width in terms of millis, Default
											// is -1 which is full history

		return this;
	}

	@Override
	public Component setTumblingWindow(int windowRange) {
		WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
		_tumblingWindowSize = windowRange * 1000;// For tumbling semantics
		return this;
	}

}