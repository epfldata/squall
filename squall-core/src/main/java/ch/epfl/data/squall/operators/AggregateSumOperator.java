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

package ch.epfl.data.squall.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.expressions.Subtraction;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import ch.epfl.data.squall.expressions.Addition;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.storage.WindowAggregationStorage;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.visitors.OperatorVisitor;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class AggregateSumOperator<T extends Number & Comparable<T>> extends OneToOneOperator implements
	AggregateOperator<T>, AggregateStream {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(AggregateSumOperator.class);

    // the GroupBy type
    private static final int GB_UNSET = -1;
    private static final int GB_COLUMNS = 0;
    private static final int GB_PROJECTION = 1;

    private DistinctOperator _distinct;
    private int _groupByType = GB_UNSET;
    private List<Integer> _groupByColumns = new ArrayList<Integer>();
    private ProjectOperator _groupByProjection;
    private int _numTuplesProcessed = 0;

    private final NumericType _wrapper;
    private final ValueExpression<T> _ve;
    private BasicStore<T> _storage;

    private final Map _map;

    private int _windowRangeSecs = -1;
    private int _slideRangeSecs = -1;

    public AggregateSumOperator(ValueExpression<T> ve, Map map) {
	_wrapper = (NumericType) ve.getType();
	_ve = ve;
	_map = map;
	_storage = new AggregationStorage<T>(this, _wrapper, _map, true);
    }

    @Override
    public void accept(OperatorVisitor ov) {
	ov.visit(this);
    }

    private boolean alreadySetOther(int GB_COLUMNS) {
	return (_groupByType != GB_COLUMNS && _groupByType != GB_UNSET);
    }

    @Override
    public void clearStorage() {
	_storage.reset();
    }

    // for this method it is essential that HASH_DELIMITER, which is used in
    // tupleToString method,
    // is the same as DIP_GLOBAL_ADD_DELIMITER
    @Override
    public List<String> getContent() {
	final String str = _storage.getContent();
	return str == null ? null : Arrays.asList(str.split("\\r?\\n"));
    }

    @Override
    public DistinctOperator getDistinct() {
	return _distinct;
    }

    @Override
    public List<ValueExpression> getExpressions() {
	final List<ValueExpression> result = new ArrayList<ValueExpression>();
	result.add(_ve);
	return result;
    }

    @Override
    public List<Integer> getGroupByColumns() {
	return _groupByColumns;
    }

    @Override
    public ProjectOperator getGroupByProjection() {
	return _groupByProjection;
    }

    private String getGroupByStr() {
	final StringBuilder sb = new StringBuilder();
	sb.append("(");
	for (int i = 0; i < _groupByColumns.size(); i++) {
	    sb.append(_groupByColumns.get(i));
	    if (i == _groupByColumns.size() - 1)
		sb.append(")");
	    else
		sb.append(", ");
	}
	return sb.toString();
    }

    @Override
    public int getNumTuplesProcessed() {
	return _numTuplesProcessed;
    }

    @Override
    public BasicStore getStorage() {
	return _storage;
    }

    @Override
    public Type getType() {
	return _wrapper;
    }

    @Override
    public boolean hasGroupBy() {
	return _groupByType != GB_UNSET;
    }

    @Override
    public boolean isBlocking() {
	return true;
    }

    @Override
    public String printContent() {
	return _storage.getContent();
    }

    // from Operator
    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
	_numTuplesProcessed++;
	if (_distinct != null) {
	    tuple = _distinct.processOne(tuple, lineageTimestamp);
	    if (tuple == null)
		return null;
	}
	String tupleHash;
	if (_groupByType == GB_PROJECTION)
	    tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
		    _groupByProjection.getExpressions(), _map);
	else
	    tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
		    _map);
	final T value = _storage.update(tuple, tupleHash, lineageTimestamp);
	final String strValue = _wrapper.toString(value);

	// propagate further the affected tupleHash-tupleValue pair
	final List<String> affectedTuple = new ArrayList<String>();
	affectedTuple.add(tupleHash);
	affectedTuple.add(strValue);

	return affectedTuple;
    }

    @Override
    public List<List<String>> updateStream(List<String> tuple, boolean withMultiplicity) {
        _numTuplesProcessed++;
        if (_distinct != null) {
            tuple = _distinct.processOne(tuple, 0);
            if (tuple == null)
                return null;
        }
        String tupleHash;
        if (_groupByType == GB_PROJECTION)
            tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
                    _groupByProjection.getExpressions(), _map);
        else
            tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
                    _map);

        // might consider to split the tupleHash based on delimiter instead as this does not take into account GB_PROJECTION
        final List<String> tupleKey = new ArrayList<>(_groupByColumns.size());
        for (int i = 0; i < _groupByColumns.size(); i++)
            tupleKey.add(tuple.get(_groupByColumns.get(i)));

        // get old value
        T oldValue = null;
        List<T> currentValues = _storage.access(tupleHash);
        if (currentValues != null && currentValues.size() > 0) {
            oldValue = currentValues.get(0);
        }

        // new value after process
        final T newValue = _storage.update(tuple, tupleHash, 0);

        if (withMultiplicity) {
            List<List<String>> output = new ArrayList<List<String>>();
            if (oldValue != null) {
                List<String> oldTuple = createUpdateTuple("-1", tupleKey, _wrapper.toString(oldValue));
                output.add(oldTuple);
            }
            List<String> newTuple = createUpdateTuple("1", tupleKey, _wrapper.toString(newValue));
            output.add(newTuple);
            return output;

        } else {
            Subtraction<T> s = new Subtraction<T>(new ValueSpecification<T>(_wrapper, newValue),
                                                    new ValueSpecification<T>(_wrapper, oldValue));
            T delta = s.eval(null);
            return Arrays.asList(createUpdateTuple(null, tupleKey, _wrapper.toString(delta)));
        }

    }

    private List<String> createUpdateTuple(String multiplicity, List<String> tupleKey, String tupleValue) {
        List<String> updateTuple = new ArrayList<String>();

        updateTuple.addAll(tupleKey);
        updateTuple.add(tupleValue);

        if (multiplicity != null)
            updateTuple.add(multiplicity);
        return updateTuple;
    }

    // actual operator implementation
    @Override
    public T runAggregateFunction(T value, List<String> tuple) {
	final ValueExpression<T> base = new ValueSpecification<T>(_wrapper,
		value);
	final Addition<T> result = new Addition<T>(base, _ve);
	return result.eval(tuple);
    }

    @Override
    public T runAggregateFunction(T value1, T value2) {
	final ValueExpression<T> ve1 = new ValueSpecification<T>(_wrapper,
		value1);
	final ValueExpression<T> ve2 = new ValueSpecification<T>(_wrapper,
		value2);
	final Addition<T> result = new Addition<T>(ve1, ve2);
	return result.eval(null);
    }

    @Override
    public AggregateSumOperator setDistinct(DistinctOperator distinct) {
	_distinct = distinct;
	return this;
    }

    @Override
    public AggregateSumOperator<T> setGroupByColumns(int... hashIndexes) {
	return setGroupByColumns(Arrays
		.asList(ArrayUtils.toObject(hashIndexes)));
    }

    // from AgregateOperator
    @Override
    public AggregateSumOperator<T> setGroupByColumns(
	    List<Integer> groupByColumns) {
	if (!alreadySetOther(GB_COLUMNS)) {
	    _groupByType = GB_COLUMNS;
	    _groupByColumns = groupByColumns;
	    _storage.setSingleEntry(false);
	    return this;
	} else
	    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public AggregateSumOperator setGroupByProjection(
	    ProjectOperator groupByProjection) {
	if (!alreadySetOther(GB_PROJECTION)) {
	    _groupByType = GB_PROJECTION;
	    _groupByProjection = groupByProjection;
	    _storage.setSingleEntry(false);
	    return this;
	} else
	    throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("AggregateSumOperator with VE: ");
	sb.append(_ve.toString());
	if (_groupByColumns.isEmpty() && _groupByProjection == null)
	    sb.append("\n  No groupBy!");
	else if (!_groupByColumns.isEmpty())
	    sb.append("\n  GroupByColumns are ").append(getGroupByStr())
		    .append(".");
	else if (_groupByProjection != null)
	    sb.append("\n  GroupByProjection is ")
		    .append(_groupByProjection.toString()).append(".");
	if (_distinct != null)
	    sb.append("\n  It also has distinct ").append(_distinct.toString());
	return sb.toString();
    }

    @Override
    public AggregateOperator<T> SetWindowSemantics(int windowRangeInSeconds,
	    int windowSlideInSeconds) {
	WindowSemanticsManager._IS_WINDOW_SEMANTICS = true;
	_windowRangeSecs = windowRangeInSeconds;
	_slideRangeSecs = windowSlideInSeconds;
	_storage = new WindowAggregationStorage<>(this, _wrapper, _map, true,
		_windowRangeSecs, _slideRangeSecs);
	if (_groupByColumns != null || _groupByProjection != null)
	    _storage.setSingleEntry(false);
	return this;
    }

    @Override
    public AggregateOperator<T> SetWindowSemantics(int windowRangeInSeconds) {
	return SetWindowSemantics(windowRangeInSeconds, windowRangeInSeconds);
    }

    @Override
    public int[] getWindowSemanticsInfo() {
	int[] res = new int[2];
	res[0] = _windowRangeSecs;
	res[1] = _slideRangeSecs;
	return res;
    }

}
