/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.components.dbtoaster;

import ch.epfl.data.squall.api.sql.util.ParserUtil;
import ch.epfl.data.squall.api.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.AggregateStream;
import ch.epfl.data.squall.types.DateLongType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.Type;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBToasterJoinComponentBuilder {
    private List<Component> _relations = new LinkedList<Component>();
    private Map<String, Type[]> _relColTypes = new HashMap<String, Type[]>();
    private Map<String, String[]> _relColNames = new HashMap<String, String[]>();
    private Set<String> _relMultiplicity = new HashSet<String>();
    private Map<String, AggregateStream> _relAggregators = new HashMap<String, AggregateStream>();
    private String _sql;
    private Pattern _sqlVarPattern = Pattern.compile("([A-Za-z0-9_]+)\\.f([0-9]+)");
    private String _name;
    private Map _conf;

    public DBToasterJoinComponentBuilder() {

    }

    public DBToasterJoinComponentBuilder(Map conf) {
        this._conf = conf;
    }

    /**
     * <p>
     *     Add relation whose incoming tuples will be added to DBToaster instance.
     * </p>
     * @param relation parent relation
     * @param types incoming tuples' field types which will be used to convert from String-based tuple to typed tuple
     * @return
     *
     * @see ch.epfl.data.squall.dbtoaster.DBToasterEngine#receiveTuple
     * @see ch.epfl.data.squall.storm_components.dbtoaster.StormDBToasterJoin#performJoin
     */
    public DBToasterJoinComponentBuilder addRelation(Component relation, Type... types) {
        _relations.add(relation);
        _relColTypes.put(relation.getName(), types);
        return this;
    }

    public DBToasterJoinComponentBuilder addRelation(Component relation, Type[] types, String[] columnNames) {
        _relColNames.put(relation.getName(), columnNames);
        return addRelation(relation, types);
    }


    /**
     * <p>
     *     Add relation whose tuples have multiplicity fields.
     *     The multiplicity field provides the capability to taken back, i.e, remove the tuple which has been sent previously
     *     This is useful when the tuple previously sent is no longer valid
     * </p>
     * <p>
     *     The multiplicity field have to be at the first index and has value of 1 or -1.
     *     The DBToaster instance will be invoked with the corresponding Tuple Operation, Insert or Delete respectively
     * </p>
     * <p>
     *     Tuples from the relation are <b>broadcasted</b> to all tasks of this component
     * </p>
     *
     * @param relation parent relation
     * @param types the types array, <b>offset the multiplicity field</b>
     * @return
     *
     * @see ch.epfl.data.squall.dbtoaster.DBToasterEngine#receiveTuple
     * @see ch.epfl.data.squall.storm_components.dbtoaster.StormDBToasterJoin#performJoin
     * @see ch.epfl.data.squall.storm_components.dbtoaster.StormDBToasterJoin#attachEmitters
     */
    public DBToasterJoinComponentBuilder addRelationWithMultiplicity(Component relation, Type... types) {
        _relMultiplicity.add(relation.getName());
        return addRelation(relation, types);
    }

    public DBToasterJoinComponentBuilder addRelationWithMultiplicity(Component relation, Type[] types, String[] columnNames) {
        _relColNames.put(relation.getName(), columnNames);
        
        return addRelationWithMultiplicity(relation, types);
    }

    /**
     * <p>
     *     Add relation which is resulted from aggregation.
     *     As a result the incoming tuple should be aggregated from all tasks for the relation before processing.
     *     Here the default aggregator use {@link ch.epfl.data.squall.operators.AggregateSumOperator} which implements the {@link ch.epfl.data.squall.operators.AggregateStream} interface.
     * </p>
     * <p>
     *     Because online aggregation can invalidate previously sent tuple, the tuple from aggregate operator is output with multiplicity.
     *     Therefore, this method in turn invokes {@link #addRelationWithMultiplicity}.
     * </p>
     * <p>
     *     <b>This method can be refactored to accept other aggregator</b> like AggregateAvgOperator which need to implement {@link ch.epfl.data.squall.operators.AggregateStream}
     * </p>
     * @param relation parent relation
     * @param types
     * @return
     *
     * @see ch.epfl.data.squall.storm_components.dbtoaster.StormDBToasterJoin#processNonLastTuple
     * @see ch.epfl.data.squall.operators.AggregateStream
     */
    public DBToasterJoinComponentBuilder addAggregatedRelation(Component relation, Type... types) {
        int valueCol = types.length - 1;
        int[] groupByCols = new int[types.length - 1];
        for (int i = 0; i < groupByCols.length; i++)
            groupByCols[i] = i;
        _relAggregators.put(relation.getName(), new AggregateSumOperator(new ColumnReference(types[valueCol], valueCol), _conf).setGroupByColumns(groupByCols));

        return addRelationWithMultiplicity(relation, types);
    }

    public DBToasterJoinComponentBuilder addAggregatedRelation(Component relation, Type[] types, String[] columnNames) {
        _relColNames.put(relation.getName(), columnNames);
        return addAggregatedRelation(relation, types);
    }

    private boolean parentRelationExists(String name) {
        boolean exist = false;
        for (Component rel : _relations) {
            if (rel.getName().equals(name)) {
                exist = true;
                break;
            }
        }
        return exist;
    }

    private void validateTables(List<Table> tables) {
        for (Table table : tables) {
            String tableName = table.getName();
            if (!parentRelationExists(tableName)) {
                throw new RuntimeException("Invalid table name: " + tableName + " in the SQL query");
            }
        }
    }

    private void validateSelectItems(List<SelectItem> items) {
        for (SelectItem item : items) {
            String itemName = item.toString();
            Matcher matcher = _sqlVarPattern.matcher(itemName);
            while (matcher.find()) {
                String tableName = matcher.group(1);
                int fieldId = Integer.parseInt(matcher.group(2));

                if (!parentRelationExists(tableName)) {
                    throw new RuntimeException("Invalid table name: " + tableName + " in the SQL query");
                }

                if (fieldId < 0 || fieldId >= _relColTypes.get(tableName).length) {
                    throw new RuntimeException("Invalid field f" + fieldId + " in table: " + tableName);
                }

            }
        }
    }

    private void validateSQL(String sql) {
        SQLVisitor parsedQuery = ParserUtil.parseQuery(sql);
        List<Table> tables = parsedQuery.getTableList();
        validateTables(tables);
        List<SelectItem> items = parsedQuery.getSelectItems();
        validateSelectItems(items);
    }

    /**
     * The conversion of SquallType to SQL type used in DBToaster's sql input is based on DBToaster's sql syntax
     * @param type
     * @return
     */
    private String getSQLTypeFromTypeConversion(Type type) {
        if (type instanceof LongType || type instanceof IntegerType) {
            return "int";
        } else if (type instanceof DoubleType) {
            return "float";
        } else if (type instanceof DateLongType) { // DBToaster's generated code uses Long for Date type.
            return "date";
        } else {
            return "String";
        }
    }

    private String generateSchemaSQL() {
        StringBuilder schemas = new StringBuilder();

        for (String relName : _relColTypes.keySet()) {
            schemas.append("CREATE STREAM ").append(relName).append("(");
            Type[] colTypes = _relColTypes.get(relName);
            for (int i = 0; i < colTypes.length; i++) {
                schemas.append("f").append(i).append(" ").append(getSQLTypeFromTypeConversion(colTypes[i]));
                if (i != colTypes.length - 1) schemas.append(",");
            }
            schemas.append(") FROM FILE '' LINE DELIMITED csv;\n");
        }
        return schemas.toString();
    }

    public void setSQL(String sql) {
        validateSQL(sql);
        this._sql = generateSchemaSQL() + sql;
    }

    public void setComponentName(String name) {
        this._name = name;
    }

    public DBToasterJoinComponent build() {
        if (this._name == null) {
            // componentName
            StringBuilder nameBuilder = new StringBuilder();
            for (Component com : _relations) {
                if (nameBuilder.length() != 0) nameBuilder.append("_");
                nameBuilder.append(com.getName());
            }
            _name = nameBuilder.toString();
        }
        return new DBToasterJoinComponent(_relations, _relColTypes, _relColNames, _relMultiplicity, _relAggregators, _sql, _name);
    }

}
