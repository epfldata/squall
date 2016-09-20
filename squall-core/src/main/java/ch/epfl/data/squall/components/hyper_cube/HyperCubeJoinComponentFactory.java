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
package ch.epfl.data.squall.components.hyper_cube;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.types.Type;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;


public class HyperCubeJoinComponentFactory {
    private Map<String, Predicate> joinPredicate;
	private Component[] parents;

    private List<Component> _relations = new LinkedList<Component>();
    private Map<String, Type[]> _relColTypes = new HashMap<String, Type[]>();
    private Map<String, String[]> _relColNames = new HashMap<String, String[]>();
    private Set<String> _randomColumns = new HashSet<String>();

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


    public HyperCubeJoinComponentFactory(Component[] parents) {
    	this.parents = parents;
    	this.joinPredicate = new HashMap<String, Predicate>();
    }

    public void addPredicate(String key, Predicate pred) {
    	joinPredicate.put(key, pred);
    }

    public HyperCubeJoinComponent createHyperCubeJoinOperator() {
        return new HyperCubeJoinComponent(parents, joinPredicate,
            _relColTypes, _relColNames, _randomColumns);
    }
}
