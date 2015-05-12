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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.hyper_cube.StormHyperCubeJoin;
import ch.epfl.data.squall.storm_components.theta.StormThetaJoin;
import ch.epfl.data.squall.storm_components.theta.StormThetaJoinBDB;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.JoinerComponent;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormBoltComponent;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.utilities.MyUtilities;

public class HyperCubeJoinComponent extends JoinerComponent implements Component {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(HyperCubeJoinComponent.class);
    private ArrayList<Component> parents;
    private String componentName = "_";
    private long batchOutputMillis;
    private final ChainOperator chain = new ChainOperator();
    private Component child;
    private StormBoltComponent joiner;
    private List<ValueExpression> hashExpressions;
    private List<Integer> hashIndexes;
    private Predicate joinPredicate;
    private boolean printOut;
    private InterchangingComponent interComp = null;
    private boolean printOutSet; // whether printOut was already set
    private Type contentSensitiveThetaJoinWrapper = null;


    public HyperCubeJoinComponent(ArrayList<Component> parents) {
        this.parents = parents;
        for (Component tmp : this.parents) {
            tmp.setChild(this);
            componentName += tmp.getName() + "_";
        }
    }

    @Override
    public HyperCubeJoinComponent add(Operator operator) {
        chain.addOperator(operator);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Component)
            return componentName.equals(((Component) obj).getName());
        else
            return false;
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources() {
        final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
        for (final Component parent : getParents())
            list.addAll(parent.getAncestorDataSources());
        return list;
    }


    @Override
    public long getBatchOutputMillis() {
        return batchOutputMillis;
    }

    @Override
    public ChainOperator getChainOperator() {
        return chain;
    }

    @Override
    public Component getChild() {
        return child;
    }

    // from StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
        return joiner.getEmitterIDs();
    }

    @Override
    public List<String> getFullHashList() {
        throw new RuntimeException(
                "Load balancing for Theta join is done inherently!");
    }

    @Override
    public List<ValueExpression> getHashExpressions() {
        return hashExpressions;
    }

    @Override
    public List<Integer> getHashIndexes() {
        return hashIndexes;
    }

    @Override
    public String getInfoID() {
        return joiner.getInfoID();
    }

    public Predicate getJoinPredicate() {
        return joinPredicate;
    }

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public Component[] getParents() {
        return parents.toArray(new Component[parents.size()]);
    }

    @Override
    public boolean getPrintOut() {
        return printOut;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash
                + (componentName != null ? componentName.hashCode() : 0);
        return hash;
    }


    @Override
    public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
                          List<String> allCompNames, Config conf, int hierarchyPosition) {
        MyUtilities.checkBatchOutput(batchOutputMillis,
		chain.getAggregation(), conf);

        // _joiner = new StormThetaJoin();
        ArrayList<StormEmitter> emitters = new ArrayList<StormEmitter>();
        for (StormEmitter se : parents)
            emitters.add(se);

//        joiner = new NaiveJoiner(emitters.get(0), emitters.get(1), this,
//                allCompNames, joinPredicate, false,
//                hierarchyPosition, builder, killer, conf, interComp,
//                false, contentSensitiveThetaJoinWrapper);
//        joiner = new StormHyperCubeJoin(emitters, this, allCompNames, joinPredicate,
//                hierarchyPosition, builder, killer, conf, interComp, contentSensitiveThetaJoinWrapper);

    }
    @Override
    public HyperCubeJoinComponent setBatchOutputMillis(long millis) {
        batchOutputMillis = millis;
        return this;
    }

    @Override
    public void setChild(Component child) {
        this.child = child;
    }

    // list of distinct keys, used for direct stream grouping and load-balancing
    // ()
    @Override
    public HyperCubeJoinComponent setFullHashList(List<String> fullHashList) {
        throw new RuntimeException(
                "Load balancing for Theta join is done inherently!");
    }

    @Override
    public HyperCubeJoinComponent setHashExpressions(
            List<ValueExpression> hashExpressions) {
        this.hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public HyperCubeJoinComponent setOutputPartKey(List<Integer> hashIndexes) {
        this.hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public HyperCubeJoinComponent setOutputPartKey(int... hashIndexes) {
        return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
    }

    @Override
    public HyperCubeJoinComponent setInterComp(InterchangingComponent inter) {
        interComp = inter;
        return this;
    }

    @Override
    public HyperCubeJoinComponent setJoinPredicate(Predicate joinPredicate) {
        this.joinPredicate = joinPredicate;
        return this;
    }

    @Override
    public HyperCubeJoinComponent setPrintOut(boolean printOut) {
        this.printOutSet = true;
        this.printOut = printOut;
        return this;
    }


    @Override
    public HyperCubeJoinComponent setContentSensitiveThetaJoinWrapper(
            Type wrapper) {
        contentSensitiveThetaJoinWrapper = wrapper;
        return this;
    }

    @Override
    public Component setSlidingWindow(int windowRange) {
        WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
        _windowSize = windowRange * 1000; // Width in terms of millis, Default
        // is -1 which is full history

        return this;
    }

    @Override
    public Component setTumblingWindow(int windowRange) {
        WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
        _tumblingWindowSize = windowRange * 1000;// For tumbling semantics
        return this;
    }
}
