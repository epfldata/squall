/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.schema.Table;


public class TableAliasName {

    //alias, Table
    private HashMap<String, Table> _nameTableList = new HashMap<String, Table>();
    
    //alias, schemaName
    private HashMap<String, String> _nameSchemaList = new HashMap<String, String>();
    
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
