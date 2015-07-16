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
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormOperator;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

public class OperatorComponent extends RichComponent<OperatorComponent> {
    protected OperatorComponent getThis() {
      return this;
    }

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(OperatorComponent.class);

    private final String _componentName;

    // private Component _parent;

    private List<String> _fullHashList;

    private ArrayList<Component> _parents;

    public OperatorComponent(Component parent, String componentName) {
	_parents = new ArrayList<Component>(1);
	parent.setChild(this);
	_parents.add(parent);
	_componentName = componentName;
    }

    public OperatorComponent(ArrayList<Component> parents, String componentName) {
	_parents = parents;
	for (Component parent : parents) {
	    parent.setChild(this);
	}

	_componentName = componentName;
    }

    @Override
    public List<String> getFullHashList() {
	return _fullHashList;
    }

    @Override
    public String getName() {
	return _componentName;
    }

    @Override
    public Component[] getParents() {
	// return new Component[] { _parent };
	Component[] res = new Component[_parents.size()];
	int index = 0;
	for (Component parent : _parents) {
	    res[index] = parent;
	    index++;
	}
	return res;

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

	// _stormOperator = new StormOperator(_parent, this, allCompNames,
	// hierarchyPosition, builder, killer, conf);
	setStormEmitter(new StormOperator(_parents, this, allCompNames,
                                          hierarchyPosition, builder, killer, conf));
    }

    @Override
    public OperatorComponent setFullHashList(List<String> fullHashList) {
	_fullHashList = fullHashList;
	return this;
    }

    @Override
    public Component setInterComp(InterchangingComponent inter) {
	throw new RuntimeException(
		"Operator component does not support setInterComp");
    }
}
