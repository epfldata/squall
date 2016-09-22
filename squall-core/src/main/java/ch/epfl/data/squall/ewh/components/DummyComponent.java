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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.ewh.storm_components.DummyBolt;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

public class DummyComponent implements Component {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DummyComponent.class);

    private final String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private final ChainOperator _chain = new ChainOperator();

    private boolean _printOut;
    private boolean _printOutSet;

    private final Component _parent;
    private Component _child;
    private DummyBolt _dummyBolt;

    private List<String> _fullHashList;

    public DummyComponent(Component parent, String componentName) {
	_parent = parent;
	_parent.setChild(this);

	_componentName = componentName;
    }

    @Override
    public DummyComponent add(Operator operator) {
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
	list.addAll(_parent.getAncestorDataSources());
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

    // from StormComponent
    @Override
    public String[] getEmitterIDs() {
	return _dummyBolt.getEmitterIDs();
    }

    @Override
    public List<String> getFullHashList() {
	return _fullHashList;
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
	return _dummyBolt.getInfoID();
    }

    @Override
    public String getName() {
	return _componentName;
    }

    @Override
    public Component[] getParents() {
	return new Component[] { _parent };
    }

    @Override
    public boolean getPrintOut() {
	return _printOut;
    }

    @Override
    public int hashCode() {
	int hash = 5;
	hash = 47 * hash
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

	_dummyBolt = new DummyBolt(_parent, this, allCompNames,
		hierarchyPosition, builder, killer, conf);
    }

    @Override
    public DummyComponent setBatchOutputMillis(long millis) {
	_batchOutputMillis = millis;
	return this;
    }

    @Override
    public void setChild(Component child) {
	_child = child;
    }

    @Override
    public DummyComponent setFullHashList(List<String> fullHashList) {
	_fullHashList = fullHashList;
	return this;
    }

    @Override
    public DummyComponent setHashExpressions(
	    List<ValueExpression> hashExpressions) {
	_hashExpressions = hashExpressions;
	return this;
    }

    @Override
    public DummyComponent setOutputPartKey(List<Integer> hashIndexes) {
	_hashIndexes = hashIndexes;
	return this;
    }

    @Override
    public DummyComponent setOutputPartKey(int... hashIndexes) {
	_hashIndexes = Arrays.asList(ArrayUtils.toObject(hashIndexes));
	return this;
    }

    @Override
    public DummyComponent setPrintOut(boolean printOut) {
	_printOutSet = true;
	_printOut = printOut;
	return this;
    }

    @Override
    public Component setContentSensitiveThetaJoinWrapper(Type wrapper) {
	return this;
    }

}
