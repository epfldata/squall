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
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.JoinerComponent;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.Predicate;
import ch.epfl.data.squall.storm_components.InterchangingComponent;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.dbtoaster.StormDBToasterJoin;
import ch.epfl.data.squall.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DBToasterJoinComponent extends JoinerComponent implements Component {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DBToasterJoinComponent.class);


    private Component _child;

    private final String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private StormEmitter _joiner;
    private Predicate _joinPredicate;

    private final ChainOperator _chain = new ChainOperator();

    private boolean _printOut;
    private boolean _printOutSet; // whether printOut was already set

    private List<String> _fullHashList;

    private List<Component> _parents;
    private Map<String, Type[]> _parentNameColTypes;
    private String _equivalentSQL;
    private boolean _outputWithMultiplicity;

    protected DBToasterJoinComponent(List<Component> relations, Map<String, Type[]> relationTypes, String sql, String name) {
        _parents = relations;
        for (Component comp : _parents) {
            comp.setChild(this);
        }
        _parentNameColTypes = relationTypes;
        _componentName = name;
        _equivalentSQL = sql;
    }

    @Override
    public DBToasterJoinComponent add(Operator operator) {
        _chain.addOperator(operator);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Component)
            return _componentName.equals(((Component) obj).getName());
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
        return _batchOutputMillis;
    }

    @Override
    public ChainOperator getChainOperator() {
        return _chain;
    }

    @Override
    public Component getChild() {
        return _child;
    }

    // from StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
        return _joiner.getEmitterIDs();
    }

    @Override
    public List<String> getFullHashList() {
        return _fullHashList;
    }

    @Override
    public List<ValueExpression> getHashExpressions() {
        return _hashExpressions;
    }

    @Override
    public List<Integer> getHashIndexes() {
        return _hashIndexes;
    }

    @Override
    public String getInfoID() {
        return _joiner.getInfoID();
    }

    @Override
    public String getName() {
        return _componentName;
    }

    @Override
    public Component[] getParents() {
        return _parents.toArray(new Component[_parents.size()]);
    }

    @Override
    public boolean getPrintOut() {
        return _printOut;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash
                + (_componentName != null ? _componentName.hashCode() : 0);
        return hash;
    }

    @Override
    public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
                          List<String> allCompNames, Config conf, int hierarchyPosition) {

        // by default print out for the last component
        // for other conditions, can be set via setPrintOut
        if (hierarchyPosition == StormComponent.FINAL_COMPONENT
                && !_printOutSet)
            setPrintOut(true);

        MyUtilities.checkBatchOutput(_batchOutputMillis,
                _chain.getAggregation(), conf);

        _joiner = new StormDBToasterJoin(getParents(), this,
                allCompNames,
                _parentNameColTypes,
                hierarchyPosition,
                builder, killer, conf, _outputWithMultiplicity);
    }

    @Override
    public DBToasterJoinComponent setBatchOutputMillis(long millis) {
        _batchOutputMillis = millis;
        return this;
    }

    @Override
    public void setChild(Component child) {
        _child = child;
    }

    @Override
    public Component setContentSensitiveThetaJoinWrapper(Type wrapper) {
        return null;
    }


    // list of distinct keys, used for direct stream grouping and load-balancing
    // ()
    @Override
    public DBToasterJoinComponent setFullHashList(List<String> fullHashList) {
        _fullHashList = fullHashList;
        return this;
    }

    @Override
    public DBToasterJoinComponent setHashExpressions(
            List<ValueExpression> hashExpressions) {
        _hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public DBToasterJoinComponent setOutputPartKey(List<Integer> hashIndexes) {
        _hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public DBToasterJoinComponent setOutputPartKey(int... hashIndexes) {
        return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
    }

    @Override
    public DBToasterJoinComponent setPrintOut(boolean printOut) {
        _printOutSet = true;
        _printOut = printOut;
        return this;
    }

    @Override
    public Component setInterComp(InterchangingComponent inter) {
        throw new RuntimeException(
                "DBToasterJoin component does not support setInterComp");
    }

    @Override
    public DBToasterJoinComponent setJoinPredicate(Predicate predicate) {
        _joinPredicate = predicate;
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

    /**
     * Whether to output tuple with a multiplicity field.
     * <p>
     * If this is set to true, the stream of output tuples contains an additional "multiplicity" field at the first index.
     * Multiplicity value +1 represents added tuple while multiplicity -1 represent deleted tuple
     * </p>
     * <p>
     * If this is not set, the stream of output tuples does not have "multiplicity". The value field of tuple will be the change delta between consecutive tuples
     * </p>
     * @param withMul
     */
    public Component setOutputWithMultiplicity(boolean withMul) {
        _outputWithMultiplicity = withMul;
        return this;
    }

    public String getSQLQuery() {
        return _equivalentSQL;
    }

}
