package sql.schema;

import plan_runner.conversion.TypeConversion;
import java.util.HashMap;
import java.util.List;


public class Schema {
    protected HashMap<String, List<ColumnNameType>> tables = new HashMap<String, List<ColumnNameType>>();
    protected HashMap<String, Integer> tableSize = new HashMap<String, Integer>();

    //fullSchemaColumnName, numDistinctValues
    protected HashMap<String, Integer> _columnDistinctValues = new HashMap<String, Integer>();

    //fullSchemaColumnName, Range
    protected HashMap<String, Range> _columnRanges = new HashMap<String, Range>();


    public List<ColumnNameType> getTableSchema(String tableName){
        return tables.get(tableName);
    }

    /*
     * For a field N1.NATIONNAME, tableSchemaName is NATION, column is NATIONNAME
     */
    public TypeConversion getType(String tableSchemaName, String columnName){
        List<ColumnNameType> table = tables.get(tableSchemaName);
        if(table == null){
            throw new RuntimeException("Table " + tableSchemaName + "doesn't exist!");
        }
        for(ColumnNameType cnt: table){
            if (cnt.getName().equals(columnName)){
                return cnt.getType();
            }
        }
        throw new RuntimeException("Column " + columnName + " doesn't exist within " + tableSchemaName);
    }

    public boolean contains(String tableName, String column) {
        List<ColumnNameType> columns = tables.get(tableName);
        for(ColumnNameType cnt: columns){
            if(cnt.getName().equals(column)){
                return true;
            }
        }
        return false;
    }

    /********
     * CARDINALITY METHODS
     * ******
     */
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
