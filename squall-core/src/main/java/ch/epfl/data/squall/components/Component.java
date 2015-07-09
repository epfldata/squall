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

import java.io.Serializable;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.Type;

public interface Component extends Serializable, ComponentProperties,
	StormEmitter {

    public Component add(Operator operator); // add to the end of
    // ChainOperator

    public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
	    List<String> allCompNames, Config conf, int hierarchyPosition);

    // sending the content of the component every 'millis' milliseconds
    public Component setBatchOutputMillis(long millis);

    // methods necessary for query plan processing
    public void setChild(Component child);

    public Component setContentSensitiveThetaJoinWrapper(Type wrapper);

    // method necessary for direct grouping and load balancing:
    // at receiver side:
    public Component setFullHashList(List<String> fullHashList);

    public Component setHashExpressions(List<ValueExpression> hashExpressions);

    public Component setInterComp(InterchangingComponent inter);

    public Component setOutputPartKey(int... hashIndexes); // this is a shortcut

    // this needs to be separately kept, due to
    // Parser.SelectItemsVisitor.ComplexCondition
    // in short, whether the component uses indexes or expressions
    // is also dependent on on other component taking part in a join
    public Component setOutputPartKey(List<Integer> hashIndexes);

    public Component setPrintOut(boolean printOut);

}
