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

import ch.epfl.data.squall.conversion.StringConversion;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class ProjectOperator implements Operator {
	private static final long serialVersionUID = 1L;

	private List<ValueExpression> _veList = new ArrayList<ValueExpression>();

	private int _numTuplesProcessed = 0;

	public ProjectOperator(int... projectIndexes) {
		for (final int columnNumber : projectIndexes) {
			final ColumnReference columnReference = new ColumnReference(
					new StringConversion(), columnNumber);
			_veList.add(columnReference);
		}
	}

	public ProjectOperator(List<ValueExpression> veList) {
		_veList = veList;
	}

	public ProjectOperator(ValueExpression... veArray) {
		_veList.addAll(Arrays.asList(veArray));
	}

	@Override
	public void accept(OperatorVisitor ov) {
		ov.visit(this);
	}

	@Override
	public List<String> getContent() {
		throw new RuntimeException(
				"getContent for ProjectionOperator should never be invoked!");
	}

	public List<ValueExpression> getExpressions() {
		return _veList;
	}

	@Override
	public int getNumTuplesProcessed() {
		return _numTuplesProcessed;
	}

	@Override
	public boolean isBlocking() {
		return false;
	}

	@Override
	public String printContent() {
		throw new RuntimeException(
				"printContent for ProjectionOperator should never be invoked!");
	}

	@Override
	public List<String> process(List<String> tuple, long lineageTimestamp) {
		_numTuplesProcessed++;
		final List<String> projection = new ArrayList<String>();
		for (final ValueExpression ve : _veList) {
			final String columnContent = ve.evalString(tuple);
			projection.add(columnContent);
		}
		return projection;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("ProjectOperator ");
		if (!_veList.isEmpty())
			sb.append("(");
		for (int i = 0; i < _veList.size(); i++) {
			sb.append(_veList.get(i).toString());
			if (i == _veList.size() - 1)
				sb.append(")");
			else
				sb.append(", ");
		}
		return sb.toString();
	}
}