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


package ch.epfl.data.squall.api.sql.optimizers.index;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import ch.epfl.data.squall.api.sql.schema.ColumnNameType;
import ch.epfl.data.squall.api.sql.schema.Schema;
import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.util.TableAliasName;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;

public class IndexTranslator {
	private final Schema _schema;
	private final TableAliasName _tan;

	public IndexTranslator(Schema schema, TableAliasName tan) {
		_schema = schema;
		_tan = tan;
	}

	public boolean contains(List<ColumnNameType> tupleSchema, String columnName) {
		final int index = indexOf(tupleSchema, columnName);
		return index != ParserUtil.NOT_FOUND;
	}

	public int getChildIndex(int originalIndex, Component originator,
			Component requestor) {
		final Component child = originator.getChild();
		final Component[] parents = child.getParents();

		if (child.getChainOperator().getProjection() != null)
			throw new RuntimeException(
					"Cannot use getChildIndex method on the component with Projection! getOutputSize does not work anymore!");

		int index = originalIndex;

		if (parents.length < 2)
			// no changes, only one parent
			return index;

		// only right parent changes the index
		final Component leftParent = parents[0];
		final Component rightParent = parents[1];

		if (rightParent.equals(originator))
			if (!rightParent.getHashIndexes().contains(originalIndex)) {
				// requested column is *not* in joinColumns
				final int indexesBefore = ParserUtil.getNumElementsBefore(
						originalIndex, rightParent.getHashIndexes());
				index = ParserUtil.getPreOpsOutputSize(leftParent, _schema,
						_tan) - indexesBefore + originalIndex;
			} else {
				// requested column is in joinColumns
				// if in the keys have to find lhs index
				final int joinIndex = rightParent.getHashIndexes().indexOf(
						originalIndex);
				index = leftParent.getHashIndexes().get(joinIndex);
			}

		if (child.equals(requestor))
			return index;
		else
			return getChildIndex(index, originator.getChild(), requestor);
	}

	/*
	 * For a given component and column, find out the index of that column in a
	 * given component. not meant to be used with projections - EarlyProjection
	 * is the very last thing done on the plan tupleSchema is not used here
	 * (it's used for Cost-based optimizer, where each component updates the
	 * schema after each operator)
	 */
	public int getColumnIndex(Column column, Component requestor) {
		final String columnName = column.getColumnName();
		final String tblCompName = ParserUtil.getComponentName(column);
		final String tableSchemaName = _tan.getSchemaName(tblCompName);
		final List<ColumnNameType> columns = _schema
				.getTableSchema(tableSchemaName);

		final int originalIndex = indexOf(columns, columnName);

		// finding originator by name in the list of ancestors
		final List<DataSourceComponent> sources = requestor
				.getAncestorDataSources();
		Component originator = null;
		for (final DataSourceComponent source : sources)
			if (source.getName().equals(tblCompName)) {
				originator = source;
				break;
			}

		if (requestor.equals(originator))
			return originalIndex;
		else
			return getChildIndex(originalIndex, originator, requestor);
	}

	/*
	 * Not used outside this class. For a field N1.NATIONNAME, columnName is
	 * NATIONNAME List<ColumnNameType> is from a Schema Table (TPCH.nation)
	 */
	public int indexOf(List<ColumnNameType> tupleSchema, String columnName) {
		for (int i = 0; i < tupleSchema.size(); i++)
			if (tupleSchema.get(i).getName().equals(columnName))
				return i;
		return ParserUtil.NOT_FOUND;
	}

	/*
	 * Is component already hashed by hashIndexes (does its parent sends tuples
	 * hashed by hashIndexes). hashIndexes are indexes wrt component. If returns
	 * true not only if hashes are equivalent, but also if the parent groups
	 * tuples exactly the same as the affected component, with addition of some
	 * more columns. This means that Join and Aggregation can be performed on
	 * the same node. Inspiration taken from the Nephele paper.
	 */
	public boolean isHashedBy(Component component, List<Integer> hashIndexes) {
		final Component[] parents = component.getParents();
		if (parents != null) {
			// if both parents have only hashIndexes, they point to the same
			// indexes in the child
			// so we choose arbitrarily first parent
			final Component parent = parents[0];
			final List<Integer> parentHashes = parent.getHashIndexes();
			if (parent.getHashExpressions() == null) {
				final List<Integer> parentHashIndexes = new ArrayList<Integer>();
				for (final int parentHash : parentHashes)
					parentHashIndexes.add(getChildIndex(parentHash, parent,
							component));
				return isSuperset(parentHashIndexes, hashIndexes);
			}
		}
		// if there are HashExpressions, we don't bother to do analysis, we know
		// it's false
		return false;
	}

	private boolean isSuperset(List<Integer> parentHashIndexes,
			List<Integer> affectedHashIndexes) {
		final int parentSize = parentHashIndexes.size();
		final int affectedSize = affectedHashIndexes.size();

		if (parentSize < affectedSize)
			return false;
		else if (parentSize == affectedSize)
			return parentHashIndexes.equals(affectedHashIndexes);
		else {
			// parent partitions more than necessary for a child
			for (int i = 0; i < affectedSize; i++)
				if (!(affectedHashIndexes.get(i).equals(parentHashIndexes
						.get(i))))
					return false;
			return true;
		}
	}

}
