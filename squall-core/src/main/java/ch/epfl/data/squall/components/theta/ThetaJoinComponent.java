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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.AbstractJoinerComponent;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.StormBoltComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.storm_components.theta.StormThetaJoin;
import ch.epfl.data.squall.storm_components.theta.StormThetaJoinBDB;
import ch.epfl.data.squall.storm_components.hyper_cube.TradionalTwoWayJoin;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ManualHybridHyperCubeAssignment.Dimension;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;

public class ThetaJoinComponent extends AbstractJoinerComponent<ThetaJoinComponent> {
    protected ThetaJoinComponent getThis() {
      return this;
    }

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(ThetaJoinComponent.class);
    private final Component _firstParent;
    private final Component _secondParent;
    private boolean _isContentSensitive;
    private Type _contentSensitiveThetaJoinWrapper = null;


    private Map<String, String[]> _relColNames;
    private Map<String, Dimension> _dimensions;
    private Set<String> _randomColumns;
    private Map<String, Type[]> _parentNameColTypes;

    // equi-weight histogram
    private boolean _isPartitioner;

    public ThetaJoinComponent(Component firstParent, Component secondParent,
      boolean isContentSensitive) {
      super(new Component[]{firstParent, secondParent});
      _firstParent = firstParent;
      _secondParent = secondParent;
      _isContentSensitive = isContentSensitive;
    }

    public ThetaJoinComponent(Component firstParent, Component secondParent,
	    boolean isContentSensitive, Map<String, Type[]> relationTypes, 
      Map<String, String[]> relColNames, Map<String, Dimension> dimensions, 
      Set<String> randomColumns) {


      super(new Component[]{firstParent, secondParent});
    	_firstParent = firstParent;
    	_secondParent = secondParent;
    	_isContentSensitive = isContentSensitive;

      _parentNameColTypes = relationTypes;
      _relColNames = relColNames;
      _dimensions = dimensions;
      _randomColumns = randomColumns;
    }

    @Override
    public List<String> getFullHashList() {
	throw new RuntimeException(
		"Load balancing for Theta join is done inherently!");
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

	boolean isBDB = MyUtilities.isBDB(conf);
	if (isBDB && getJoinPredicate() == null) {
	    throw new RuntimeException(
		    "Please provide joinPredicate if you want to run BDB!");
	}

  StormBoltComponent joiner;
  if (MyUtilities.isHypercube(conf)) {
          joiner = new TradionalTwoWayJoin(_firstParent, _secondParent, _relColNames, 
                                        _parentNameColTypes, _randomColumns, this, allCompNames, 
                                        getJoinPredicate(), _isPartitioner, hierarchyPosition, builder, killer, conf,
                                      _isContentSensitive, _contentSensitiveThetaJoinWrapper);
  } else if (isBDB && (hierarchyPosition == StormComponent.FINAL_COMPONENT)) {
          joiner = new StormThetaJoinBDB(_firstParent, _secondParent, this,
                                         allCompNames, getJoinPredicate(), hierarchyPosition, builder,
                                         killer, conf);
  } else {
          joiner = new StormThetaJoin(_firstParent, _secondParent, this,
                                      allCompNames, getJoinPredicate(), _isPartitioner,
                                      hierarchyPosition, builder, killer, conf,
                                      _isContentSensitive, _contentSensitiveThetaJoinWrapper);
	}
	if (getSlidingWindow() > 0 || getTumblingWindow() > 0) {
          joiner.setWindowSemantics(getSlidingWindow(), getTumblingWindow());
        }

        setStormEmitter(joiner);
    }

    @Override
    public ThetaJoinComponent setContentSensitiveThetaJoinWrapper(Type wrapper) {
	_contentSensitiveThetaJoinWrapper = wrapper;
	return this;
    }

    // list of distinct keys, used for direct stream grouping and load-balancing
    // ()
    @Override
    public ThetaJoinComponent setFullHashList(List<String> fullHashList) {
	throw new RuntimeException(
		"Load balancing for Theta join is done inherently!");
    }

    public ThetaJoinComponent setPartitioner(boolean isPartitioner) {
	_isPartitioner = isPartitioner;
	return this;
    }
}
