/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.components.dbtoaster;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.JoinerComponent;
import ch.epfl.data.squall.components.AbstractJoinerComponent;
import ch.epfl.data.squall.operators.AggregateStream;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.dbtoaster.StormDBToasterJoin;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DBToasterJoinComponent extends AbstractJoinerComponent<DBToasterJoinComponent> {
    protected DBToasterJoinComponent getThis() {
      return this;
    }

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DBToasterJoinComponent.class);

    private Map<String, String[]> _relColNames;
    private Map<String, Type[]> _parentNameColTypes;
    private Set<String> _parentsWithMultiplicity;
    private Map<String, AggregateStream> _parentsWithAggregator;
    private String _equivalentSQL;

    protected DBToasterJoinComponent(List<Component> relations, Map<String, Type[]> relationTypes, Map<String, String[]> relColNames,
                                     Set<String> relationsWithMultiplicity, Map<String, AggregateStream>  relationsWithAggregator,
                                     String sql, String name) {
      super(relations, name);
        _parentsWithMultiplicity = relationsWithMultiplicity;
        _parentsWithAggregator = relationsWithAggregator;
        _parentNameColTypes = relationTypes;
        _relColNames = relColNames;
        _equivalentSQL = sql;
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

        setStormEmitter(new StormDBToasterJoin(getParents(), this,
                                               allCompNames,
                                               _parentNameColTypes,
                                               _relColNames,
                                               _parentsWithMultiplicity,
                                               _parentsWithAggregator,
                                               hierarchyPosition,
                                               builder, killer, conf));
    }

    @Override
    public DBToasterJoinComponent setJoinPredicate(Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    public String getSQLQuery() {
        return _equivalentSQL;
    }

}
