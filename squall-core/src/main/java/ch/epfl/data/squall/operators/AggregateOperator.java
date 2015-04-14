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

import java.util.List;

import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.types.Type;

public interface AggregateOperator<T> extends Operator {
    public void clearStorage();

    public DistinctOperator getDistinct();

    // this is null for AggregateCountOperator
    public List<ValueExpression> getExpressions();

    public List<Integer> getGroupByColumns();

    public ProjectOperator getGroupByProjection();

    // internal storage class
    public BasicStore getStorage();

    public AggregateOperator<T> SetWindowSemantics(int windowRangeInSeconds,
	    int windowSlideInSeconds);

    public AggregateOperator<T> SetWindowSemantics(int windowRangeInSeconds);

    public int[] getWindowSemanticsInfo();

    public Type getType();

    public boolean hasGroupBy();

    public T runAggregateFunction(T value, List<String> tuple);

    public T runAggregateFunction(T value1, T value2);

    // SUM(DISTINCT ValueExpression), COUNT(DISTINCT ValueExpression): a single
    // ValueExpression by SQL standard
    // MySQL supports multiple ValueExpression. Inside aggregation(SUM, COUNT),
    // there must be single ValueExpression.
    public AggregateOperator setDistinct(DistinctOperator distinct);

    public AggregateOperator setGroupByColumns(int... groupByColumns);

    // GROUP BY ValueExpression is not part of the SQL standard, only columns
    // can be set.
    public AggregateOperator setGroupByColumns(List<Integer> groupByColumns);

    public AggregateOperator setGroupByProjection(ProjectOperator projection);

    // HAVING clause: Since HAVING depends on aggregate result,
    // it cannot be evaluated before all the tuples are processed.
    // This will be done by user manually, as well as ORDER BY clause.

    // COUNT(COLUMN_NAME): an AggregateCountOperator *preceeded* by a
    // SelectionOperator.
    // this counts non-null COLUMN_NAMEs
    // due to the order of operators, this is not doable in our system

}
