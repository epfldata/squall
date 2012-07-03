package sql.util;

import java.util.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/*
 * This class is necessary because we cannot extract tableSchemaName from column.getTable()
 *   tableSchemaName is the name from TPCH_Schema (non-aliased tableName)
 */
public class TableAliasName {
    private String _queryName; 
    
    //alias, schemaName
    private HashMap<String, String> _nameSchemaList = new HashMap<String, String>();

    /*
     * This list of tables is provided from SQLVisitor
     *   It won't work if we obtain a list of tables from a list of columns
     *   using the method column.getTable()
     */
    public TableAliasName(List<Table> tableList, String queryName){
        _queryName = queryName;
        for(Table table: tableList){
            String tableCompName = ParserUtil.getComponentName(table);
            String tableSchemaName = table.getWholeTableName();
            _nameSchemaList.put(tableCompName, tableSchemaName);
        }
    }

    public String getSchemaName(String tableCompName){
        String schemaName = _nameSchemaList.get(tableCompName);
        if(schemaName == null){
            throw new RuntimeException("Table with alias " + tableCompName + 
                    " does not exist in " + _queryName + " query!");
        }
        return schemaName;
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
