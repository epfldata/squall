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

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.JoinerComponent;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ManualHybridHyperCubeAssignment.Dimension;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;

public class ThetaJoinComponentFactory {

    private List<Component> _relations = new LinkedList<Component>();
    private Map<String, Type[]> _relColTypes = new HashMap<String, Type[]>();
    private Map<String, String[]> _relColNames = new HashMap<String, String[]>();
    private Map<String, Dimension> _dimensions = new HashMap<String, Dimension>();
    private Set<String> _randomColumns = new HashSet<String>();
    private Map _conf;


    public ThetaJoinComponentFactory(Map conf) {
    	_conf = conf;
    }

    public ThetaJoinComponentFactory() {

    }

    public void addRelation(Component relation, Type... types) {
        _relations.add(relation);
        _relColTypes.put(relation.getName(), types);
    }

    public void addRelation(Component relation, Type[] types, String[] columnNames) {
        _relColNames.put(relation.getName(), columnNames);
        addRelation(relation, types);
    }

    public void addRelation(Component relation, 
        Type[] types, String[] columnNames, int[] randomColumns) {
        
        for(int i : randomColumns) {
            _randomColumns.add(columnNames[i]);
        }

        _relColNames.put(relation.getName(), columnNames);
        addRelation(relation, types);
    }

	public JoinerComponent createThetaJoinOperator(Component firstParent, Component secondParent,
	    QueryBuilder queryBuilder) {
        JoinerComponent result = new ThetaJoinComponent(firstParent, secondParent, false, 
			_relColTypes, _relColNames, _dimensions, _randomColumns);

        queryBuilder.add(result);

        return result;
	}


    public static JoinerComponent createThetaJoinOperator(int thetaJoinType,
	    Component firstParent, Component secondParent,
	    QueryBuilder queryBuilder) {
	JoinerComponent result = null;
	if (thetaJoinType == SystemParameters.STATIC_CIS) {
	    result = new ThetaJoinComponent(firstParent, secondParent, false);
	} else if (thetaJoinType == SystemParameters.STATIC_CS) {
	    result = new ThetaJoinComponent(firstParent, secondParent, true);
	} else if (thetaJoinType == SystemParameters.EPOCHS_CIS || thetaJoinType == SystemParameters.EPOCHS_CS) {
          if (secondParent == null) {
	    result = new AdaptiveThetaJoinComponent(firstParent);
          } else {
	    result = new AdaptiveThetaJoinComponent(firstParent, secondParent);
          }
	} else {
	    throw new RuntimeException("Unsupported Thtea Join Type");
	}
	queryBuilder.add(result);
	return result;
    }
}
