/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package schema;

import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class Schema {
    protected HashMap<String, List<ColumnNameType>> tables = new HashMap<String, List<ColumnNameType>>();
    protected HashMap<String, Integer> tableSize = new HashMap<String, Integer>();

    public boolean contains(String tableName, String column) {
        List<ColumnNameType> columns = tables.get(tableName);
        for(ColumnNameType cnt: columns){
            if(cnt.getName().equals(column)){
                return true;
            }
        }
        return false;
    }

    public int indexOf(String tableName, String column) {
        List<ColumnNameType> columns = tables.get(tableName);
        return indexOf(columns, column);
    }

    public int indexOf(List<ColumnNameType> columns, String column){
        for(int i=0; i<columns.size(); i++){
            if(columns.get(i).getName().equals(column)){
                return i;
            }
        }
        return -1;
    }

    public List<ColumnNameType> getColumnNameTypes(String tableName){
        return tables.get(tableName);
    }

    /*
     * Schema contains only names of the columns from a table
     */
    public List<String> getTableSchema(String tableName){
        List<ColumnNameType> columnTypes = getColumnNameTypes(tableName);

        List<String> columnNames = new ArrayList<String>();
        for(ColumnNameType cnt: columnTypes){
            columnNames.add(cnt.getName());
        }
        return columnNames;
    }

    public TypeConversion getType(String tableName, String columnName){
        List<ColumnNameType> table = tables.get(tableName);
        if(table == null){
            throw new RuntimeException("Table " + tableName + "doesn't exist!");
        }
        for(ColumnNameType cnt: table){
            if (cnt.getName().equals(columnName)){
                return cnt.getType();
            }
        }
        throw new RuntimeException("Column " + columnName + " doesn't exist within " + tableName);
    }

    public int getTableSize(String table){
        if(!tableSize.containsKey(table)){
            throw new RuntimeException("Table " + table + "does not exist!");
        }
        return tableSize.get(table);
    }

}
