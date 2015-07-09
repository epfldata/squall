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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.storage.KeyValueStore;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormDstJoin;
import ch.epfl.data.squall.storm_components.StormDstTupleStorageBDB;
import ch.epfl.data.squall.storm_components.StormDstTupleStorageJoin;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class EquiJoinComponent extends RichJoinerComponent<EquiJoinComponent> {
    protected EquiJoinComponent getThis() {
      return this;
    }

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(EquiJoinComponent.class);

    private final Component _firstParent;
    private final Component _secondParent;

    private final String _componentName;

    private StormEmitter _joiner;

    // The storage is actually KeyValue<String, String>
    // or AggregationStorage<Numeric> for pre-aggregation
    // Access method returns a list of Strings (a list of Numerics for
    // pre-aggregation)
    private BasicStore<String> _firstStorage, _secondStorage;
    // preAggregation
    private ProjectOperator _firstPreAggProj, _secondPreAggProj;

    private List<String> _fullHashList;
    private Predicate _joinPredicate;

    private boolean _isRemoveIndex = true;

    public EquiJoinComponent(Component firstParent, Component secondParent) {
	_firstParent = firstParent;
	_firstParent.setChild(this);
	_secondParent = secondParent;
	_secondParent.setChild(this);

	_componentName = firstParent.getName() + "_" + secondParent.getName();
    }

    public EquiJoinComponent(Component firstParent, Component secondParent,
	    boolean isRemoveIndex) {
	_firstParent = firstParent;
	_firstParent.setChild(this);
	_secondParent = secondParent;
	_secondParent.setChild(this);
	_componentName = firstParent.getName() + "_" + secondParent.getName();
	_isRemoveIndex = isRemoveIndex;
    }

    public EquiJoinComponent(Component firstParent, int firstJoinIndex,
	    Component secondParent, int secondJoinIndex) {
	this(firstParent, secondParent);
	firstParent.setOutputPartKey(firstJoinIndex);
	secondParent.setOutputPartKey(secondJoinIndex);
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources() {
	final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
	for (final Component parent : getParents())
	    list.addAll(parent.getAncestorDataSources());
	return list;
    }

    // from StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
	return _joiner.getEmitterIDs();
    }

    @Override
    public List<String> getFullHashList() {
	return _fullHashList;
    }

    @Override
    public String getInfoID() {
	return _joiner.getInfoID();
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
    public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
	    List<String> allCompNames, Config conf, int hierarchyPosition) {

	// by default print out for the last component
	// for other conditions, can be set via setPrintOut
	if (hierarchyPosition == StormComponent.FINAL_COMPONENT
		&& !getPrintOutSet())
	    setPrintOut(true);

	MyUtilities.checkBatchOutput(getBatchOutputMillis(),
                                     getChainOperator().getAggregation(), conf);

	// If not set in Preaggregation, we set normal storages
	if (_firstStorage == null)
	    _firstStorage = new KeyValueStore<String, String>(conf);
	if (_secondStorage == null)
	    _secondStorage = new KeyValueStore<String, String>(conf);

	boolean isBDB = MyUtilities.isBDB(conf);
	if (isBDB && _joinPredicate == null) {
	    throw new RuntimeException(
		    "Please provide _joinPredicate if you want to run BDB!");
	}

	// TODO: what is with the if condition
	if (isBDB && (hierarchyPosition == StormComponent.FINAL_COMPONENT)) {
	    _joiner = new StormDstTupleStorageBDB(_firstParent, _secondParent,
		    this, allCompNames, _joinPredicate, hierarchyPosition,
		    builder, killer, conf);
	} else if (_joinPredicate != null) {
	    _joiner = new StormDstTupleStorageJoin(_firstParent, _secondParent,
		    this, allCompNames, _joinPredicate, hierarchyPosition,
		    builder, killer, conf);
	} else {
	    // should issue a warning
	    _joiner = new StormDstJoin(_firstParent, _secondParent, this,
		    allCompNames, _firstStorage, _secondStorage,
		    _firstPreAggProj, _secondPreAggProj, hierarchyPosition,
		    builder, killer, conf, _isRemoveIndex);
	}
    }

    // Out of the first storage (join of S tuple with R relation)
    public EquiJoinComponent setFirstPreAggProj(ProjectOperator firstPreAggProj) {
	_firstPreAggProj = firstPreAggProj;
	return this;
    }

    // next four methods are for Preaggregation
    public EquiJoinComponent setFirstPreAggStorage(
	    AggregationStorage firstPreAggStorage) {
	_firstStorage = firstPreAggStorage;
	return this;
    }

    // list of distinct keys, used for direct stream grouping and load-balancing
    // ()
    @Override
    public EquiJoinComponent setFullHashList(List<String> fullHashList) {
	_fullHashList = fullHashList;
	return this;
    }

    @Override
    public JoinerComponent setInterComp(InterchangingComponent inter) {
	throw new RuntimeException(
		"EquiJoin component does not support setInterComp");
    }

    @Override
    public EquiJoinComponent setJoinPredicate(Predicate predicate) {
	_joinPredicate = predicate;
	return this;
    }

    // Out of the second storage (join of R tuple with S relation)
    public EquiJoinComponent setSecondPreAggProj(
	    ProjectOperator secondPreAggProj) {
	_secondPreAggProj = secondPreAggProj;
	return this;
    }

    public EquiJoinComponent setSecondPreAggStorage(
	    AggregationStorage secondPreAggStorage) {
	_secondStorage = secondPreAggStorage;
	return this;
    }

}
