/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.schema.Table;

/*
 * This class is necessary because we cannot extract tableSchemaName from column.getTable()
 *   tableSchemaName is the name from TPCH_Schema (non-aliased tableName)
 */
public class TableAliasName {

    //alias, Table
    private HashMap<String, Table> _nameTableList = new HashMap<String, Table>();
    
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
            _nameTableList.put(tableCompName, table);
            String tableSchemaName = table.getWholeTableName();
            _nameSchemaList.put(tableCompName, tableSchemaName);
        }
    }
    
    public Table getTable(String tableCompName){
        return _nameTableList.get(tableCompName);
    }

    public String getSchemaName(String tableCompName){
        return _nameSchemaList.get(tableCompName);
    }

}
