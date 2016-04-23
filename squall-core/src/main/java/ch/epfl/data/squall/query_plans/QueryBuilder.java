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

package ch.epfl.data.squall.query_plans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.EquiJoinComponent;
import ch.epfl.data.squall.components.theta.AdaptiveThetaJoinComponent;
import ch.epfl.data.squall.ewh.components.DummyComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;

import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.utilities.SquallContext;


public class QueryBuilder implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<Component> _plan = new ArrayList<Component>();

    public void add(Component component) {
	_plan.add(component);
    }

    // Component names are unique - alias is used for tables
    public boolean contains(String name) {
	for (final Component component : _plan)
	    if (component.getName().equals(name))
		return true;
	return false;
    }

    public DataSourceComponent createDataSource(String tableName, Map conf) {
	DataSourceComponent dsc = new DataSourceComponent(tableName, conf);
	add(dsc);
	return dsc;
    }

    public EquiJoinComponent createEquiJoin(Component firstParent,
	    Component secondParent) {
	EquiJoinComponent ejc = new EquiJoinComponent(firstParent, secondParent);
	add(ejc);
	return ejc;
    }

    public EquiJoinComponent createEquiJoin(Component firstParent,
	    Component secondParent, boolean isRemoveIndex) {
	EquiJoinComponent ejc = new EquiJoinComponent(firstParent,
		secondParent, isRemoveIndex);
	add(ejc);
	return ejc;
    }

    public Component getComponent(String name) {
	for (final Component component : _plan)
	    if (component.getName().equals(name))
		return component;
	return null;
    }

    public List<String> getComponentNames() {
	final List<String> result = new ArrayList<String>();
	for (final Component component : _plan)
	    result.add(component.getName());
	return result;
    }

    public Component getLastComponent() {
	return _plan.get(_plan.size() - 1);
    }

    public List<Component> getPlan() {
	return _plan;
    }

    public TopologyBuilder createTopology(SquallContext context) {
        Config conf = context.getConfiguration();
	TopologyBuilder builder = new TopologyBuilder();
	TopologyKiller killer = new TopologyKiller(builder);

	List<Component> queryPlan = this.getPlan();
	List<String> allCompNames = this.getComponentNames();
	Collections.sort(allCompNames);
	
    int planSize = queryPlan.size();
	for (int i = 0; i < planSize; i++) {
	    Component component = queryPlan.get(i);
        
	    Component child = component.getChild();
	    if (child == null) {
		// a last component (it might be multiple of them)
		component.makeBolts(builder, killer, allCompNames, conf,
			StormComponent.FINAL_COMPONENT);
	    } else if (child instanceof DummyComponent) {
		component.makeBolts(builder, killer, allCompNames, conf,
			StormComponent.NEXT_TO_DUMMY);
	    } else if (child.getChild() == null
		    && !(child instanceof AdaptiveThetaJoinComponent)) {
		// if the child is dynamic, then reshuffler is NEXT_TO_LAST
		component.makeBolts(builder, killer, allCompNames, conf,
			StormComponent.NEXT_TO_LAST_COMPONENT);
	    } else {
		component.makeBolts(builder, killer, allCompNames, conf,
			StormComponent.INTERMEDIATE);
	    }
	}
	
	// NEED TO UNCOMMENT FOR DBTOASTER TO WORK
	//QueryBuilderDBToaster.createDBToasterTopology(queryPlan, context, conf);
	
	// printing infoID information and returning the result
	// printInfoID(killer, queryPlan); commented out because IDs are now
	// descriptive names
	return builder;
    }


  public int getNumberFinalTasks(Config conf) {
    int numFinalTasks = 0;

    for(Component component : getPlan()) {
      if (component.getChild() == null) {
	final int parallelism = SystemParameters.getInt(conf, component.getName() + "_PAR");
        numFinalTasks = numFinalTasks + (parallelism == 0 ? 1 : parallelism);
      }
    }

    return numFinalTasks;

  }


}
