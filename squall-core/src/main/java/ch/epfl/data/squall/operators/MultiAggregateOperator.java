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

import org.apache.commons.lang.ArrayUtils;

import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.storage.BasicStore;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class MultiAggregateOperator implements AggregateOperator {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final List<AggregateOperator> _opList;

	public MultiAggregateOperator(List<AggregateOperator> opList, Map map) {
		_opList = opList;
	}

	@Override
	public void accept(OperatorVisitor ov) {
		ov.visit(this);
	}

	@Override
	public void clearStorage() {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public List<String> getContent() {
		throw new RuntimeException(
				"Preaggregation with MultiAggregateOperator does not work yet.");
	}

	@Override
	public DistinctOperator getDistinct() {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public List getExpressions() {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public List getGroupByColumns() {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public ProjectOperator getGroupByProjection() {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	private int getNumGroupByColumns(AggregateOperator agg) {
		int result = 0;
		final List<Integer> groupByColumns = agg.getGroupByColumns();
		if (groupByColumns != null)
			result += groupByColumns.size();
		final ProjectOperator groupByProjection = agg.getGroupByProjection();
		if (groupByProjection != null)
			result += groupByProjection.getExpressions().size();
		return result;
	}

	@Override
	public int getNumTuplesProcessed() {
		for (final AggregateOperator agg : _opList)
			return agg.getNumTuplesProcessed();
		// the result of the first operator, but this is the same for all
		// the AggregateOperators
		return 0;
	}

	@Override
	public BasicStore getStorage() {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public TypeConversion getType() {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public boolean hasGroupBy() {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public boolean isBlocking() {
		return true;
	}

	@Override
	public String printContent() {
		final StringBuilder sb = new StringBuilder();
		int i = 0;
		for (final AggregateOperator agg : _opList) {
			sb.append("\nAggregation ").append(i).append("\n")
					.append(agg.printContent());
			i++;
		}
		return sb.toString();
	}

	@Override
	public List<String> process(List<String> tuple, long lineageTimestamp) {
		// this will work only if they have the same groupBy
		// otherwise the result of process is not important at all
		final List<String> result = new ArrayList<String>();
		int i = 0;
		for (final AggregateOperator agg : _opList) {
			final List<String> current = agg.process(tuple, lineageTimestamp);
			if (i == 0)
				result.addAll(current);
			else {
				// for all beside the first result we exclude the groupBy
				// columns
				// because of preaggragations, there might be multiple groupBy
				// columns (it's not A-B|res, but A|B|res)
				// we know that all of them are at the beginning
				final int numGB = getNumGroupByColumns(agg);
				result.addAll(current.subList(numGB, current.size()));
			}
			i++;
		}
		return result;
	}

	@Override
	public Object runAggregateFunction(Object value, List tuple) {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public Object runAggregateFunction(Object value1, Object value2) {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public AggregateOperator setDistinct(DistinctOperator distinct) {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public AggregateOperator setGroupByColumns(int... hashIndexes) {
		return setGroupByColumns(Arrays
				.asList(ArrayUtils.toObject(hashIndexes)));
	}

	@Override
	public AggregateOperator setGroupByColumns(List groupByColumns) {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public AggregateOperator setGroupByProjection(ProjectOperator projection) {
		throw new UnsupportedOperationException(
				"You are not supposed to call this method from MultiAggregateOperator.");
	}

	@Override
	public AggregateOperator SetWindowSemantics(int windowRangeInSeconds,
			int windowSlideInSeconds) {
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public int[] getWindowSemanticsInfo() {
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public AggregateOperator SetWindowSemantics(int windowRangeInSeconds) {
		throw new RuntimeException("Not implemented yet");
	}
}
