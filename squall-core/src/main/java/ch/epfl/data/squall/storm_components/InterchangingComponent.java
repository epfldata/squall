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

package ch.epfl.data.squall.storm_components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.Type;

public class InterchangingComponent implements Component {
    private static final long serialVersionUID = 1L;
    private final Component _firstParent;
    private final Component _secondParent;
    private Component _child;
    private final String _componentName;
    private long _batchOutputMillis;
    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private InterchangingBolt _interBolt;
    private boolean _printOut;
    private final int _multFactor;

    public InterchangingComponent(Component firstParent,
	    Component secondParent, int multfactor) {
	_firstParent = firstParent;
	_firstParent.setChild(this);
	_secondParent = secondParent;
	_secondParent.setChild(this);
	_componentName = firstParent.getName() + "_" + secondParent.getName()
		+ "_INTER";
	_multFactor = multfactor;
    }

    @Override
    public Component add(Operator operator) {
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
	return null;
    }

    @Override
    public Component getChild() {
	return _child;
    }

    // from StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
	return new String[] { _componentName };
    }

    @Override
    public List<String> getFullHashList() {
	throw new RuntimeException(
		"Load balancing for Dynamic Theta join is done inherently!");
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
	return _interBolt.getInfoID();
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

	_interBolt = new InterchangingBolt(_firstParent, _secondParent, this,
		allCompNames, builder, killer, conf, _multFactor);

    }

    @Override
    public Component setBatchOutputMillis(long millis) {
	_batchOutputMillis = millis;
	return this;
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
    public Component setFullHashList(List<String> fullHashList) {
	throw new RuntimeException(
		"Load balancing for Dynamic Theta join is done inherently!");
    }

    @Override
    public Component setHashExpressions(List<ValueExpression> hashExpressions) {
	_hashExpressions = hashExpressions;
	return this;
    }

    @Override
    public Component setInterComp(InterchangingComponent inter) {
	throw new RuntimeException(
		"Interchanging component does not support InterComp");
    }

    @Override
    public Component setOutputPartKey(int... hashIndexes) {
	return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
    }

    @Override
    public Component setOutputPartKey(List<Integer> hashIndexes) {
	_hashIndexes = hashIndexes;
	return this;
    }

    @Override
    public Component setPrintOut(boolean printOut) {
	_printOut = printOut;
	return this;
    }
}
