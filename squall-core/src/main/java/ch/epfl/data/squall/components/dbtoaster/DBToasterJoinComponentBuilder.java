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
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.types.DateLongType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.PartitioningScheme;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBToasterJoinComponentBuilder {
    private List<Component> _relations = new LinkedList<Component>();
    private Map<String, ValueExpression[]> _relColRefs = new HashMap<String, ValueExpression[]>();
    private String _sql;
    private Pattern _sqlVarPattern = Pattern.compile("([A-Za-z0-9_]+)\\.f([0-9]+)");

    public DBToasterJoinComponentBuilder addRelation(Component relation, ColumnReference... columnReferences) {
        _relations.add(relation);
        ValueExpression[] cols = new ValueExpression[columnReferences.length];

        for (ColumnReference cref : columnReferences) {
            cols[cref.getColumnIndex()] = cref;
        }
        _relColRefs.put(relation.getName(), cols);
        return this;
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

                if (fieldId < 0 || fieldId >= _relColRefs.get(tableName).length) {
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

    private String getSQLTypeFromTypeConversion(Type typeConversion) {
        if (typeConversion instanceof LongType || typeConversion instanceof IntegerType) {
            return "int";
        } else if (typeConversion instanceof DoubleType) {
            return "float";
        } else if (typeConversion instanceof DateLongType) { // DBToaster code use Long for Date type.
            return "date";
        } else {
            return "String";
        }
    }

    private String generateSchemaSQL() {
        StringBuilder schemas = new StringBuilder();

        for (String relName : _relColRefs.keySet()) {
            schemas.append("CREATE STREAM ").append(relName).append("(");
            ValueExpression[] columnReferences = _relColRefs.get(relName);
            for (int i = 0; i < columnReferences.length; i++) {
                schemas.append("f").append(i).append(" ").append(getSQLTypeFromTypeConversion(columnReferences[i].getType()));
                if (i != columnReferences.length - 1) schemas.append(",");
            }
            schemas.append(") FROM FILE '' LINE DELIMITED csv;\n");
        }
        return schemas.toString();
    }

    public void setSQL(String sql) {
        validateSQL(sql);
        this._sql = generateSchemaSQL() + sql;
    }

    public void setPartitioningScheme(PartitioningScheme partScheme) {

    }

    public DBToasterJoinComponent build() {
        return new DBToasterJoinComponent(_relations, _relColRefs, _sql);
    }

}
