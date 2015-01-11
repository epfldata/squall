package ch.epfl.data.sql.util;

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
		for (final Iterator iter = _nameSchemaList.entrySet().iterator(); iter.hasNext();) {
			final Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
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
			throw new RuntimeException("Table with alias " + tableCompName + " does not exist in "
					+ _queryName + " query!");
		return schemaName;
	}

}
