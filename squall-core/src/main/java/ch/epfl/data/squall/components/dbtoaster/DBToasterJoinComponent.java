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
import ch.epfl.data.squall.components.RichJoinerComponent;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.AggregateStream;
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
import java.util.Set;

public class DBToasterJoinComponent extends RichJoinerComponent<DBToasterJoinComponent> {
    protected DBToasterJoinComponent getThis() {
      return this;
    }

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DBToasterJoinComponent.class);


    private Component _child;

    private final String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private StormEmitter _joiner;

    private final ChainOperator _chain = new ChainOperator();

    private boolean _printOut;
    private boolean _printOutSet; // whether printOut was already set

    private List<String> _fullHashList;

    private List<Component> _parents;
    private Map<String, Type[]> _parentNameColTypes;
    private Set<String> _parentsWithMultiplicity;
    private Map<String, AggregateStream> _parentsWithAggregator;
    private String _equivalentSQL;

    protected DBToasterJoinComponent(List<Component> relations, Map<String, Type[]> relationTypes,
                                     Set<String> relationsWithMultiplicity, Map<String, AggregateStream>  relationsWithAggregator,
                                     String sql, String name) {
        _parents = relations;
        _parentsWithMultiplicity = relationsWithMultiplicity;
        _parentsWithAggregator = relationsWithAggregator;
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
                _parentsWithMultiplicity,
                _parentsWithAggregator,
                hierarchyPosition,
                builder, killer, conf);
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
    public DBToasterJoinComponent setContentSensitiveThetaJoinWrapper(Type wrapper) {
        return this;
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
    public JoinerComponent setInterComp(InterchangingComponent inter) {
        throw new RuntimeException(
                "DBToasterJoin component does not support setInterComp");
    }

    @Override
    public DBToasterJoinComponent setJoinPredicate(Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    public String getSQLQuery() {
        return _equivalentSQL;
    }

}
