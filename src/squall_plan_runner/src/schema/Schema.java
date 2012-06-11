/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package schema;

import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Schema {
    protected HashMap<String, List<ColumnNameType>> tables = new HashMap<String, List<ColumnNameType>>();
    protected HashMap<String, Integer> tableSize = new HashMap<String, Integer>();

    //fullSchemaColumnName, numDistinctValues
    protected HashMap<String, Integer> _columnDistinctValues = new HashMap<String, Integer>();

    //fullSchemaColumnName, Range
    protected HashMap<String, Range> _columnRanges = new HashMap<String, Range>();

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

    public TypeConversion getType(String fullSchemaColumnName){
        String[] names = fullSchemaColumnName.split("\\.");
        return getType(names[0], names[1]);
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

    public int getNumDistinctValues(String fullSchemaColumnName){
         Integer result = _columnDistinctValues.get(fullSchemaColumnName);
         if (result == null){
             throw new RuntimeException("No cardinality information about " + fullSchemaColumnName);
         }
         return result;
    }

    public Range getRange(String fullSchemaColumnName){
         //TODO : if we don't have it, we can assume integers from [0, distinctValues]
         Range result = _columnRanges.get(fullSchemaColumnName);
         if (result == null){
             throw new RuntimeException("No cardinality information about " + fullSchemaColumnName);
         }
         return result;
    }

    public double getRatio(String firstTable, String secondTable){
        int firstSize = getTableSize(firstTable);
        int secondSize = getTableSize(secondTable);
        return (double)secondSize/firstSize;
    }

    public class Range<T>{
        private T _min, _max;

        public Range(T min, T max){
            _min = min;
            _max = max;
        }

        public T getMin(){
            return _min;
        }

        public T getMax(){
            return _max;
        }
    }

}
