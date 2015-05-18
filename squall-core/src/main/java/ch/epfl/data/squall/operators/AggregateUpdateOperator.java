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

package ch.epfl.data.squall.operators;

import ch.epfl.data.squall.expressions.Addition;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.storage.AggregationStorage;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.storage.WindowAggregationStorage;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.visitors.OperatorVisitor;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;
import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This operator only stores input tuple into an aggregation storage and update the value when a new tuple which hashed to the same key is processed.
 *
 * It can be as the last agg for DBToasterComponent to maintain the updated state of the result given the input tuples is the stream of update
 * It is compatible with how Squall LocalMergeResult work.
 *
 * T must extends Number just because of the runAggregation(v1, v2) uses Addition. This function is called when LocalMergeResult
 * merge result from multiple nodes
 */
public class AggregateUpdateOperator<T extends Number & Comparable<T>> implements AggregateOperator<T> {

    private BasicStore<T> _storage;
    private final Type<T> _wrapper;
    private final ValueExpression<T> _ve;
    private final Map _map;
    private List<Integer> _groupByColumns = new ArrayList<Integer>();
    private int _groupByType = GB_UNSET;
    private ProjectOperator _groupByProjection;
    // the GroupBy type
    private static final int GB_UNSET = -1;
    private static final int GB_COLUMNS = 0;
    private static final int GB_PROJECTION = 1;

    private int _numTuplesProcessed = 0;

    private int _windowRangeSecs = -1;
    private int _slideRangeSecs = -1;

    public AggregateUpdateOperator(ValueExpression<T> ve, Map map) {
        _wrapper = ve.getType();
        _ve = ve;
        _map = map;
        _storage = new AggregationStorage<T>(this, _wrapper, _map, false);
    }
    @Override
    public void clearStorage() {
        _storage.reset();
    }

    @Override
    public DistinctOperator getDistinct() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ValueExpression> getExpressions() {
        final List<ValueExpression> result = new ArrayList<ValueExpression>();
        return result;
    }

    @Override
    public List<Integer> getGroupByColumns() {
        return _groupByColumns;
    }

    @Override
    public ProjectOperator getGroupByProjection() {
        throw new UnsupportedOperationException();
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
        return true; // to work with LocalMergeResult
    }

    @Override
    public T runAggregateFunction(T value, List<String> tuple) {
        return _ve.eval(tuple);
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
    public AggregateOperator setDistinct(DistinctOperator distinct) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AggregateOperator setGroupByColumns(List<Integer> groupByColumns) {
        if (!alreadySetOther(GB_COLUMNS)) {
            _groupByType = GB_COLUMNS;
            _groupByColumns = groupByColumns;
            _storage.setSingleEntry(false);
            return this;
        } else
            throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public AggregateOperator setGroupByColumns(int... hashIndexes) {
        return setGroupByColumns(Arrays
                .asList(ArrayUtils.toObject(hashIndexes)));
    }

    @Override
    public AggregateOperator setGroupByProjection(ProjectOperator groupByProjection) {
        if (!alreadySetOther(GB_PROJECTION)) {
            _groupByType = GB_PROJECTION;
            _groupByProjection = groupByProjection;
            _storage.setSingleEntry(false);
            return this;
        } else
            throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public void accept(OperatorVisitor ov) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getContent() {
        final String str = _storage.getContent();
        return str == null ? null : Arrays.asList(str.split("\\r?\\n"));
    }

    @Override
    public int getNumTuplesProcessed() {
        return _numTuplesProcessed;
    }

    @Override
    public boolean isBlocking() {
        return true;
    }

    @Override
    public String printContent() {
        return _storage.getContent();
    }

    @Override
    public List<String> process(List<String> tuple, long lineageTimestamp) {
        _numTuplesProcessed++;
        String tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
                _map);
        _storage.update(tuple, tupleHash);
        return tuple;
    }

    private boolean alreadySetOther(int GB_COLUMNS) {
        return (_groupByType != GB_COLUMNS && _groupByType != GB_UNSET);
    }

    @Override
    public AggregateOperator<T> SetWindowSemantics(int windowRangeInSeconds, int windowSlideInSeconds) {
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
