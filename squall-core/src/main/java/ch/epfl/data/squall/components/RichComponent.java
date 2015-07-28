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

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.InterchangingComponent;

public abstract class RichComponent<C extends Component> implements Component {

    protected abstract C getThis();

    private final String _componentName;

    private long _batchOutputMillis;
    private final ChainOperator _chain = new ChainOperator();
    private Component _child;

    private List<ValueExpression> _hashExpressions;
    private List<Integer> _hashIndexes;
    private List<String> _fullHashList;

    private boolean _printOut;
    private boolean _printOutSet; // whether printOut condition is already set

    private StormEmitter _stormEmitter;

    private Component[] _parents;

    private InterchangingComponent _interComp;

    public RichComponent(Component parent, String componentName) {
      _componentName = componentName;
      _parents = new Component[]{parent};
      parent.setChild(this);
    }

    public RichComponent(Component[] parents, String componentName) {
      _componentName = componentName;
      _parents = null;
      if (parents != null) {
        _parents = parents.clone();
        for (Component parent : parents) {
          parent.setChild(this);
        }
      }
    }

    public RichComponent(List<Component> parents, String componentName) {
      this((Component[])parents.toArray(new Component[parents.size()]), componentName);
    }

    public RichComponent(Component[] parents) {
      this(parents, makeName(parents));
    }

    public RichComponent(List<Component> parents) {
      this((Component[])parents.toArray());
    }

    private static String makeName(Component[] parents) {
      ArrayList<String> names = new ArrayList<String>();
      for (Component parent : parents) {
        names.add(parent.getName());
      }
      return StringUtils.join(names, "_");
    }

    @Override
    public String getName() {
	return _componentName;
    }

    @Override
    public Component[] getParents() {
	return _parents;
    }

    @Override
    public boolean equals(Object obj) {
	if (obj instanceof Component)
            return getName().equals(((Component) obj).getName());
	else
	    return false;
    }

    @Override
    public long getBatchOutputMillis() {
	return _batchOutputMillis;
    }

    // Operations with chaining
    @Override
    public C setBatchOutputMillis(long millis) {
	_batchOutputMillis = millis;
	return getThis();
    }

    @Override
    public C add(Operator operator) {
	_chain.addOperator(operator);
	return getThis();
    }

    @Override
    public ChainOperator getChainOperator() {
	return _chain;
    }

    @Override
    public Component getChild() {
	return _child;
    }

    @Override
    public void setChild(Component child) {
	_child = child;
    }

    @Override
    public List<ValueExpression> getHashExpressions() {
	return _hashExpressions;
    }

    @Override
    public C setHashExpressions(
	    List<ValueExpression> hashExpressions) {
	_hashExpressions = hashExpressions;
	return getThis();
    }

    @Override
    public List<Integer> getHashIndexes() {
	return _hashIndexes;
    }

    @Override
    public boolean getPrintOut() {
	return _printOut;
    }

    protected boolean getPrintOutSet() {
	return _printOutSet;
    }

    @Override
    public C setPrintOut(boolean printOut) {
	_printOutSet = true;
	_printOut = printOut;
	return getThis();
    }

    @Override
    public int hashCode() {
	int hash = 7;
	hash = 37 * hash
		+ (getName() != null ? getName().hashCode() : 0);
	return hash;
    }

    @Override
    public C setContentSensitiveThetaJoinWrapper(Type wrapper) {
        return getThis();
    }

    @Override
    public C setOutputPartKey(int... hashIndexes) {
	return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
    }

    @Override
    public C setOutputPartKey(List<Integer> hashIndexes) {
	_hashIndexes = hashIndexes;
	return getThis();
    }

    // from StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
	return _stormEmitter.getEmitterIDs();
    }

    @Override
    public String getInfoID() {
	return _stormEmitter.getInfoID();
    }

    protected void setStormEmitter(StormEmitter emitter) {
      _stormEmitter = emitter;
    }

    protected StormEmitter getStormEmitter(StormEmitter emitter) {
      return _stormEmitter;
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources() {
	final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
	for (final Component parent : getParents())
	    list.addAll(parent.getAncestorDataSources());
	return list;
    }

    @Override
    public List<String> getFullHashList() {
	return _fullHashList;
    }

    // list of distinct keys, used for direct stream grouping and load-balancing
    // ()
    @Override
    public C setFullHashList(List<String> fullHashList) {
	_fullHashList = fullHashList;
	return getThis();
    }


    @Override
    public C setInterComp(InterchangingComponent inter) {
	_interComp = inter;
	return getThis();
    }

    public InterchangingComponent getInterComp() {
	return _interComp;
    }


}
