package ch.epfl.data.squall.api.sql.schema;

import java.util.List;
import java.util.Map;

import ch.epfl.data.squall.api.sql.schema.parser.ParseException;
import ch.epfl.data.squall.api.sql.schema.parser.SchemaParser;
import ch.epfl.data.squall.api.sql.schema.parser.SchemaParser.ColumnInfo;
import ch.epfl.data.squall.api.sql.schema.parser.SchemaParser.TableInfo;
import ch.epfl.data.squall.conversion.TypeConversion;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class Schema {

	public class Range<T> {
		private final T _min, _max;

		public Range(T min, T max) {
			_min = min;
			_max = max;
		}

		public T getMax() {
			return _max;
		}

		public T getMin() {
			return _min;
		}
	}

	private final String _path;

	private final double _scallingFactor;

	private Map<String, TableInfo> _schemaInfo;

	public Schema(Map map) {
		this(SystemParameters.getString(map, "DIP_SCHEMA_PATH"),
				SystemParameters.getDouble(map, "DIP_DB_SIZE"));
	}

	public Schema(String path, double scallingFactor) {
		_path = path;
		_scallingFactor = scallingFactor;

		try {
			_schemaInfo = SchemaParser.getSchemaInfo(_path, _scallingFactor);
		} catch (final ParseException ex) {
			final String err = MyUtilities.getStackTrace(ex);
			throw new RuntimeException("Schema " + _path
					+ " cannot be parsed!\n " + err);
		}
	}

	public boolean contains(String fullSchemaColumnName) {
		final ColumnInfo column = getColumnInfo(fullSchemaColumnName);
		return (column != null);
	}

	private ColumnInfo getColumnInfo(String fullSchemaColumnName) {
		final String[] parts = fullSchemaColumnName.split("\\.");
		final String tableSchemaName = parts[0];
		final String columnName = parts[1];
		return getColumnInfo(tableSchemaName, columnName);
	}

	/*
	 * There is no raising exception if column does not exist it might be called
	 * from contains(), so each caller has to be aware that this method might
	 * return null
	 */
	private ColumnInfo getColumnInfo(String tableSchemaName, String columnName) {
		final TableInfo table = getTableInfo(tableSchemaName);
		final ColumnInfo column = table.getColumnInfos().get(columnName);
		return column;
	}

	public long getNumDistinctValues(String fullSchemaColumnName) {
		final ColumnInfo column = getColumnInfo(fullSchemaColumnName);
		final long distinct = column.getDistinctValues();
		if (distinct == SchemaParser.INVALID)
			throw new RuntimeException(
					"No information about the number of distinct values for column "
							+ fullSchemaColumnName
							+ "\n Either add required information to schema "
							+ _path
							+ " ,"
							+ "\n or try NMPL optimizer, which does not require any cardinality information.");
		return distinct;
	}

	public Range getRange(String fullSchemaColumnName) {
		// TODO : if we don't have it, we can assume integers from [0,
		// distinctValues]

		final ColumnInfo column = getColumnInfo(fullSchemaColumnName);
		final Object min = column.getMinValue();
		final Object max = column.getMaxValue();
		if (min == null || max == null)
			throw new RuntimeException(
					"No complete information about ranges for column "
							+ fullSchemaColumnName
							+ "\n Either add required information to schema "
							+ _path
							+ " ,"
							+ "\n or try NMPL optimizer, which does not require any cardinality information.");
		final Range range = new Range(min, max);
		return range;
	}

	public double getRatio(String firstTable, String secondTable) {
		final long firstSize = getTableSize(firstTable);
		final long secondSize = getTableSize(secondTable);
		return (double) secondSize / firstSize;
	}

	// helper methods, interface to Parser classes
	private TableInfo getTableInfo(String tableSchemaName) {
		final TableInfo table = _schemaInfo.get(tableSchemaName);
		if (table == null)
			throw new RuntimeException("Table " + tableSchemaName
					+ " does not exist in Schema \n " + _path + " !");
		return table;
	}

	public List<ColumnNameType> getTableSchema(String tableSchemaName) {
		return getTableInfo(tableSchemaName).getTableSchema();
	}

	/********
	 * CARDINALITY METHODS ******
	 */
	public long getTableSize(String tableSchemaName) {
		final TableInfo table = getTableInfo(tableSchemaName);
		final long tableSize = table.getTableSize();
		if (tableSize == SchemaParser.INVALID)
			throw new RuntimeException(
					"No information about size for table "
							+ tableSchemaName
							+ "\n Either add required information to schema "
							+ _path
							+ " ,"
							+ "\n or try NMPL optimizer, which does not require any cardinality information.");
		return tableSize;
	}

	/*
	 * For a field N1.NATIONNAME, fullSchemaColumnName is NATION.NATIONNAME
	 */
	public TypeConversion getType(String fullSchemaColumnName) {
		final ColumnInfo column = getColumnInfo(fullSchemaColumnName);
		if (column == null)
			throw new RuntimeException("Column " + fullSchemaColumnName
					+ " does not exist !");
		return column.getType();
	}

}