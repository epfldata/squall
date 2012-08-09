package sql.schema;

import java.util.List;
import java.util.Map;
import plan_runner.conversion.TypeConversion;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.SystemParameters;
import sql.schema.parser.ParseException;
import sql.schema.parser.SchemaParser;
import sql.schema.parser.SchemaParser.ColumnInfo;
import sql.schema.parser.SchemaParser.TableInfo;


public class Schema {
    private static long INVALID = -1;
    
    private String _path;
    private double _scallingFactor;
    
    private Map<String, TableInfo> _schemaInfo;    
    
    public Schema(Map map) {
        this(SystemParameters.getString(map, "DIP_SCHEMA_PATH"), SystemParameters.getDouble(map, "DIP_DB_SIZE"));        
    }
    
    public Schema(String path, double scallingFactor){
        _path = path;
        _scallingFactor = scallingFactor;
        
        try {
            _schemaInfo = SchemaParser.getSchemaInfo(_path, _scallingFactor);
        } catch (ParseException ex) {
            String err = MyUtilities.getStackTrace(ex);
            throw new RuntimeException("Schema " + _path + " cannot be parsed!\n " + err);
        }        
    }
    
    public List<ColumnNameType> getTableSchema(String tableSchemaName){
        return getTableInfo(tableSchemaName).getTableSchema();
    }    
    
    /*
     * For a field N1.NATIONNAME, tableSchemaName is NATION, column is NATIONNAME
     */
    public TypeConversion getType(String tableSchemaName, String columnName){
        ColumnInfo column = getColumnInfo(tableSchemaName, columnName);
        if(column == null){
            throw new RuntimeException("Column " + columnName + " does not exist in " + tableSchemaName + " !");
        }        
        return column.getType();
    }

    public boolean contains(String tableSchemaName, String columnName) {
        ColumnInfo column = getColumnInfo(tableSchemaName, columnName);
        return (column != null);
    }

    /********
     * CARDINALITY METHODS
     * ******
     */
    public long getTableSize(String tableSchemaName){
        TableInfo table = getTableInfo(tableSchemaName);
        long tableSize = table.getTableSize();
        if(tableSize == INVALID){
            throw new RuntimeException("No information about size for table " + tableSchemaName);
        }
        return tableSize;
    }

    public long getNumDistinctValues(String fullSchemaColumnName){
        ColumnInfo column = getColumnInfo(fullSchemaColumnName);
        long distinct = column.getDistinctValues();
        if(distinct == INVALID){
             throw new RuntimeException("No information about the number of distinct values for column " + fullSchemaColumnName);
        }
        return distinct;
    }

    public Range getRange(String fullSchemaColumnName){
         //TODO : if we don't have it, we can assume integers from [0, distinctValues]
        
        ColumnInfo column = getColumnInfo(fullSchemaColumnName);
        Object min = column.getMinValue();
        Object max = column.getMaxValue();
        if(min == null || max == null){
            throw new RuntimeException("No complete information about ranges for column " + fullSchemaColumnName);
        }
        Range range = new Range(min, max);
        return range;
    }

    public double getRatio(String firstTable, String secondTable){
        long firstSize = getTableSize(firstTable);
        long secondSize = getTableSize(secondTable);
        return (double)secondSize/firstSize;
    }
    
    //helper methods, interface to Parser classes    
    private TableInfo getTableInfo(String tableSchemaName){
        TableInfo table = _schemaInfo.get(tableSchemaName);
        if(table == null){
            throw new RuntimeException("Table " + tableSchemaName + " does not exist in Schema \n " + _path + " !");
        }
        return table;
    }

    /*
     * There is no raising exception if column does not exist
     *    it might be called from contains(),
     *    so each caller has to be aware that this method might return null
     */
    private ColumnInfo getColumnInfo(String tableSchemaName, String columnName) {
        TableInfo table = getTableInfo(tableSchemaName);
        ColumnInfo column = table.getColumnInfos().get(columnName);
        return column;
    }
    
    private ColumnInfo getColumnInfo(String fullSchemaColumnName) {
        String[] parts = fullSchemaColumnName.split("\\.");
        String tableSchemaName = parts[0];
        String columnName = parts[1];
        return getColumnInfo(tableSchemaName, columnName);
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
