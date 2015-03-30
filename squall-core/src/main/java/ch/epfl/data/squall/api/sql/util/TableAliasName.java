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


package ch.epfl.data.squall.api.sql.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/*
 * This class is necessary because we cannot extract tableSchemaName from column.getTable()
 *   tableSchemaName is the name from TPCH_Schema (non-aliased tableName)
 */
public class TableAliasName {
	private final String _queryName;

	// alias, schemaName
	private final HashMap<String, String> _nameSchemaList = new HashMap<String, String>();

	/*
	 * This list of tables is provided from SQLVisitor It won't work if we
	 * obtain a list of tables from a list of columns using the method
	 * column.getTable()
	 */
	public TableAliasName(List<Table> tableList, String queryName) {
		_queryName = queryName;
		for (final Table table : tableList) {
			final String tableCompName = ParserUtil.getComponentName(table);
			final String tableSchemaName = table.getWholeTableName();
			_nameSchemaList.put(tableCompName, tableSchemaName);
		}
	}

	/*
	 * Get all component names
	 */
	public List<String> getComponentNames() {
		final List<String> result = new ArrayList<String>();
		for (final Iterator iter = _nameSchemaList.entrySet().iterator(); iter
				.hasNext();) {
			final Map.Entry<String, String> entry = (Map.Entry<String, String>) iter
					.next();
			result.add(entry.getKey());
		}
		return result;
	}

	/*
	 * Returns "TableSchemaName.ColumnName"
	 */
	public String getFullSchemaColumnName(Column column) {
		final String columnName = column.getColumnName();
		final String tableCompName = ParserUtil.getComponentName(column);
		final String tableSchemaName = getSchemaName(tableCompName);
		return tableSchemaName + "." + columnName;
	}

	public String getSchemaName(String tableCompName) {
		final String schemaName = _nameSchemaList.get(tableCompName);
		if (schemaName == null)
			throw new RuntimeException("Table with alias " + tableCompName
					+ " does not exist in " + _queryName + " query!");
		return schemaName;
	}

}
