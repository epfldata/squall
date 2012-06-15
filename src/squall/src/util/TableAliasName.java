package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/*
 * This class is necessary because we cannot extract tableSchemaName from column.getTable()
 *   tableSchemaName is the name from TPCH_Schema (non-aliased tableName)
 */
public class TableAliasName {
    
    //alias, schemaName
    private HashMap<String, String> _nameSchemaList = new HashMap<String, String>();

    /*
     * This list of tables is provided from SQLVisitor
     *   It won't work if we obtain a list of tables from a list of columns
     *   using the method column.getTable()
     */
    public TableAliasName(List<Table> tableList){
        for(Table table: tableList){
            String tableCompName = ParserUtil.getComponentName(table);
            String tableSchemaName = table.getWholeTableName();
            _nameSchemaList.put(tableCompName, tableSchemaName);
        }
    }

    public String getSchemaName(String tableCompName){
        return _nameSchemaList.get(tableCompName);
    }

    /*
     * Returns "TableSchemaName.ColumnName"
     */
    public String getFullSchemaColumnName(Column column) {
        String columnName = column.getColumnName();
        String tableCompName = ParserUtil.getComponentName(column);
        String tableSchemaName = getSchemaName(tableCompName);
        return tableSchemaName + "." + columnName;
    }

    /*
     * Get all component names
     */
    public List<String> getComponentNames(){
        List<String> result = new ArrayList<String>();
        for (Iterator iter = _nameSchemaList.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
            result.add(entry.getKey());
        }
        return result;
    }

}
