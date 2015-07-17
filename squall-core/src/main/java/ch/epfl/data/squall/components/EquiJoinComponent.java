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

import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
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
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.MyUtilities;

public class EquiJoinComponent extends RichJoinerComponent<EquiJoinComponent> {
    protected EquiJoinComponent getThis() {
      return this;
    }

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(EquiJoinComponent.class);

    private final Component _firstParent;
    private final Component _secondParent;

    // The storage is actually KeyValue<String, String>
    // or AggregationStorage<Numeric> for pre-aggregation
    // Access method returns a list of Strings (a list of Numerics for
    // pre-aggregation)
    private BasicStore<String> _firstStorage, _secondStorage;
    // preAggregation
    private ProjectOperator _firstPreAggProj, _secondPreAggProj;

    private boolean _isRemoveIndex;

    public EquiJoinComponent(Component firstParent, Component secondParent) {
      this(firstParent, secondParent, true);
    }

    public EquiJoinComponent(Component firstParent, Component secondParent,
	    boolean isRemoveIndex) {
      super(new Component[]{firstParent, secondParent});
	_firstParent = firstParent;
	_secondParent = secondParent;
	_isRemoveIndex = isRemoveIndex;
    }

    public EquiJoinComponent(Component firstParent, int firstJoinIndex,
	    Component secondParent, int secondJoinIndex) {
	this(firstParent, secondParent);
	firstParent.setOutputPartKey(firstJoinIndex);
	secondParent.setOutputPartKey(secondJoinIndex);
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
        Predicate joinPredicate = getJoinPredicate();
	if (isBDB && joinPredicate == null) {
	    throw new RuntimeException(
		    "Please provide joinPredicate if you want to run BDB!");
	}

	// TODO: what is with the if condition
	if (isBDB && (hierarchyPosition == StormComponent.FINAL_COMPONENT)) {
          setStormEmitter(new StormDstTupleStorageBDB(_firstParent, _secondParent,
                                                      this, allCompNames, joinPredicate, hierarchyPosition,
                                                      builder, killer, conf));
	} else if (joinPredicate != null) {
	  setStormEmitter(new StormDstTupleStorageJoin(_firstParent, _secondParent,
                                                       this, allCompNames, joinPredicate, hierarchyPosition,
                                                       builder, killer, conf));
	} else {
	    // should issue a warning
          setStormEmitter(new StormDstJoin(_firstParent, _secondParent, this,
                                           allCompNames, _firstStorage, _secondStorage,
                                           _firstPreAggProj, _secondPreAggProj, hierarchyPosition,
                                           builder, killer, conf, _isRemoveIndex));
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

    @Override
    public EquiJoinComponent setInterComp(InterchangingComponent inter) {
	throw new RuntimeException(
		"EquiJoin component does not support setInterComp");
    }
}
