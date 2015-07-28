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

/**
 *
 * @author El Seidy
 * This Class is the Theta-join-Dynamic Wrapper which includes all the Theta components.
 * 1- ThetaReshuffler Bolt.
 * 2- ThetaJoiner Bolt.
 * 3- One instance of ThetaClock Spout.
 * 4- One instance of ThetaMappingAssignerSynchronizer Bolt.
 */
package ch.epfl.data.squall.components.theta;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.AbstractJoinerComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.thetajoin.adaptive.storm_component.ThetaJoinerAdaptiveAdvisedEpochs;
import ch.epfl.data.squall.thetajoin.adaptive.storm_component.ThetaReshufflerAdvisedEpochs;
import ch.epfl.data.squall.thetajoin.adaptive.storm_matrix_mapping.ThetaDataMigrationJoinerToReshufflerMapping;
import ch.epfl.data.squall.thetajoin.adaptive.storm_matrix_mapping.ThetaJoinAdaptiveMapping;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ContentInsensitiveMatrixAssignment;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class AdaptiveThetaJoinComponent extends AbstractJoinerComponent<AdaptiveThetaJoinComponent> {
    protected AdaptiveThetaJoinComponent getThis() {
      return this;
    }

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger
	    .getLogger(AdaptiveThetaJoinComponent.class);
    private final Component _firstParent;
    private final Component _secondParent;
    private ThetaReshufflerAdvisedEpochs _reshuffler;
    private int _joinerParallelism;

    public AdaptiveThetaJoinComponent(Component firstParent,
	    Component secondParent) {
      super(new Component[]{firstParent, secondParent});
	_firstParent = firstParent;
	_secondParent = secondParent;
    }


    public AdaptiveThetaJoinComponent(Component firstParent) {
      super(new Component[]{firstParent});
	_firstParent = firstParent;
	_secondParent = null;
    }

    @Override
    public List<String> getFullHashList() {
	throw new RuntimeException(
		"Load balancing for Dynamic Theta join is done inherently!");
    }

    @Override
    public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
	    List<String> allCompNames, Config conf, int hierarchyPosition) {

	// by default print out for the last component
	// for other conditions, can be set via setPrintOut
	if (hierarchyPosition == StormComponent.FINAL_COMPONENT
            && !getPrintOutSet())
	    setPrintOut(true);
	_joinerParallelism = SystemParameters.getInt(conf, getName()
		+ "_PAR");
	MyUtilities.checkBatchOutput(getBatchOutputMillis(),
                                     getChainOperator().getAggregation(), conf);

	int firstCardinality, secondCardinality;
	if (_secondParent == null) { // then first has to be of type
	    // Interchanging Emitter
	    firstCardinality = SystemParameters.getInt(conf, new String(
		    _firstParent.getName().split("-")[0] + "_CARD"));
	    secondCardinality = SystemParameters.getInt(conf, new String(
		    _firstParent.getName().split("-")[1] + "_CARD"));
	} else {
	    firstCardinality = SystemParameters.getInt(conf,
		    _firstParent.getName() + "_CARD");
	    secondCardinality = SystemParameters.getInt(conf,
		    _secondParent.getName() + "_CARD");
	}

	final ContentInsensitiveMatrixAssignment _currentMappingAssignment = new ContentInsensitiveMatrixAssignment(
		firstCardinality, secondCardinality, _joinerParallelism, -1);

	final String dim = _currentMappingAssignment.getMappingDimensions();
	// dim ="1-1"; //initiate Splitting
	LOG.info(getName() + "Initial Dimensions is: " + dim);

	// create the bolts ..

	// Create the reshuffler.
	_reshuffler = new ThetaReshufflerAdvisedEpochs(_firstParent,
		_secondParent, allCompNames, _joinerParallelism,
		hierarchyPosition, conf, builder, dim);

	// Create the Join Bolt.
        ThetaJoinerAdaptiveAdvisedEpochs joiner = new ThetaJoinerAdaptiveAdvisedEpochs(_firstParent,
                                                                                       _secondParent, this, allCompNames, getJoinPredicate(),
                                                                                       hierarchyPosition, builder, killer, conf, _reshuffler, dim);
	setStormEmitter(joiner);
	_reshuffler.setJoinerID(joiner.getID());

	/*
	 * setup communication between the components.
	 */
	// 1) Hook up the mapper&Synchronizer(reshuffler) to the reshuffler
	_reshuffler.getCurrentBolt().directGrouping(_reshuffler.getID(),
		SystemParameters.ThetaSynchronizerSignal);

	// 2) Hook up the previous emitters to the reshuffler

	final ThetaJoinAdaptiveMapping dMap = new ThetaJoinAdaptiveMapping(
		conf, -1);
	final ArrayList<StormEmitter> emittersList = new ArrayList<StormEmitter>();
        emittersList.add(_firstParent);
        if (_secondParent != null)
          emittersList.add(_secondParent);
	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		_reshuffler.getCurrentBolt().customGrouping(emitterID, dMap); // default
	    // message
	    // stream
	}
	// 3) Hook up the DataMigration from the joiners to the reshuffler
	_reshuffler.getCurrentBolt().customGrouping(joiner.getID(),
		SystemParameters.ThetaDataMigrationJoinerToReshuffler,
		new ThetaDataMigrationJoinerToReshufflerMapping(conf, -1));
	// --for the LAST_ACK !!
	joiner.getCurrentBolt().allGrouping(_reshuffler.getID());
	// 4) Hook up the signals from the reshuffler to the joiners
	joiner.getCurrentBolt().allGrouping(_reshuffler.getID(),
		SystemParameters.ThetaReshufflerSignal);
	// 5) Hook up the DataMigration from the reshuffler to the joiners
	joiner.getCurrentBolt().directGrouping(_reshuffler.getID(),
		SystemParameters.ThetaDataMigrationReshufflerToJoiner);
	// 6) Hook up the Data_Stream data (normal tuples) from the reshuffler
	// to the joiners
	joiner.getCurrentBolt().directGrouping(_reshuffler.getID(),
		SystemParameters.ThetaDataReshufflerToJoiner);
	// 7) Hook up the Acks from the Joiner to the Mapper&Synchronizer
	_reshuffler.getCurrentBolt().directGrouping(joiner.getID(),
		SystemParameters.ThetaJoinerAcks);// synchronizer is already one
	// anyway.
    }

    // TODO IMPLEMENT ME
    @Override
    public AdaptiveThetaJoinComponent setContentSensitiveThetaJoinWrapper(Type wrapper) {
	return this;
    }

    // list of distinct keys, used for direct stream grouping and load-balancing
    // ()
    @Override
    public AdaptiveThetaJoinComponent setFullHashList(List<String> fullHashList) {
	throw new RuntimeException(
		"Load balancing for Dynamic Theta join is done inherently!");
    }

}
