/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.api.sql.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Select;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.api.sql.optimizers.CompGen;
import ch.epfl.data.squall.api.sql.optimizers.name.CostParams;
import ch.epfl.data.squall.api.sql.optimizers.name.NameCompGen;
import ch.epfl.data.squall.api.sql.schema.ColumnNameType;
import ch.epfl.data.squall.api.sql.schema.Schema;
import ch.epfl.data.squall.api.sql.visitors.jsql.ColumnCollectVisitor;
import ch.epfl.data.squall.api.sql.visitors.jsql.PrintVisitor;
import ch.epfl.data.squall.api.sql.visitors.jsql.SQLVisitor;
import ch.epfl.data.squall.api.sql.visitors.squall.ColumnRefCollectVisitor;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.ChainOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

public class ParserUtil {
    /*
     * append each expr to the corresponding componentName. componentName is the
     * key for collocatedExprs.
     */
    public static void addAndExprsToComps(
	    Map<String, Expression> collocatedExprs, List<Expression> exprs) {

	for (final Expression expr : exprs) {
	    // first we determine which component it belongs to
	    // In "R.A = S.A AND T.A = 4", "R.A = S.A" is not WHERE clause, it's
	    // a join condition
	    final List<Column> columns = getJSQLColumns(expr);
	    final String componentName = getComponentName(columns.get(0)
		    .getTable());

	    addAndExprToComp(collocatedExprs, expr, componentName);
	}

    }

    /*
     * append expr to the corresponding component Used outside this class,
     * that's why compName is not extracted from expr
     */
    public static void addAndExprToComp(
	    Map<String, Expression> collocatedExprs, Expression expr,
	    String compName) {

	if (collocatedExprs.containsKey(compName)) {
	    final Expression oldExpr = collocatedExprs.get(compName);
	    final Expression newExpr = new AndExpression(oldExpr, expr);
	    collocatedExprs.put(compName, newExpr);
	} else
	    collocatedExprs.put(compName, expr);

    }

    /*
     * This has to same semantic as addAndExprToComps the only difference is
     * that collocatedExprs is a different type we need to keep all the ancestor
     * DataSources which correspond to appropriate orExpr
     */
    public static void addOrExprsToComps(
	    Map<Set<String>, Expression> collocatedExprs,
	    List<OrExpression> orExprs) {
	for (final OrExpression orExpr : orExprs)
	    addOrExprToComp(collocatedExprs, orExpr);
    }

    public static void addOrExprToComp(
	    Map<Set<String>, Expression> collocatedExprs, OrExpression expr) {
	final List<Column> columns = getJSQLColumns(expr);
	final Set<String> compNameSet = new HashSet<String>(
		getCompNamesFromColumns(columns));

	if (collocatedExprs.containsKey(compNameSet)) {
	    final Expression oldExpr = collocatedExprs.get(compNameSet);
	    final Expression newExpr = new AndExpression(oldExpr, expr);
	    collocatedExprs.put(compNameSet, newExpr);
	} else
	    collocatedExprs.put(compNameSet, expr);
    }

    public static <K, V> void addToCollection(K key, V singleValue,
	    Map<K, List<V>> collection) {
	List<V> valueList;
	if (collection.containsKey(key))
	    valueList = collection.get(key);
	else {
	    valueList = new ArrayList<V>();
	    collection.put(key, valueList);
	}
	valueList.add(singleValue);
    }

    public static void batchesToMap(NameCompGen cg, Map map) {
	for (final Component comp : cg.getQueryBuilder().getPlan()) {
	    final String compName = comp.getName();
	    final int batchSize = cg.getCostParameters(compName).getBatchSize();
	    SystemParameters.putInMap(map, compName + "_BS", batchSize);
	}
    }

    /*
     * This is a deep copy of fromColumn to toColumn
     */
    public static void copyColumn(Column toColumn, Column fromColumn) {
	toColumn.setColumnName(fromColumn.getColumnName());
	toColumn.setTable(fromColumn.getTable());
    }

    public static void copyColumn(Column toColumn, String fromColumnStr) {
	final Column fromColumn = nameToColumn(fromColumnStr);
	copyColumn(toColumn, fromColumn);
    }

    /*
     * From a list of <NATIONNAME, StringConversion> it creates a list of
     * <N1.NATIONNAME, StringConversion>
     */
    public static TupleSchema createAliasedSchema(
	    List<ColumnNameType> originalSchema, String tableCompName) {
	final List<ColumnNameType> cnts = new ArrayList<ColumnNameType>();

	for (final ColumnNameType cnt : originalSchema) {
	    String columnName = cnt.getName();
	    columnName = getFullName(tableCompName, columnName);
	    final Type tc = cnt.getType();
	    cnts.add(new ColumnNameType(columnName, tc));
	}

	return new TupleSchema(cnts);
    }

    // template method
    public static <T> List<T> createListExp(T elem) {
	final List<T> result = new ArrayList<T>();
	result.add(elem);
	return result;
    }

    public static boolean equals(Table table1, Table table2) {
	if (table1 == null)
	    return table2 == null;
	else if (table2 != null)
	    return getComponentName(table1).equals(getComponentName(table2));
	else
	    return false;
    }

    public static List<Integer> extractColumnIndexes(
	    List<ValueExpression> veList) {
	final List<Integer> indexes = new ArrayList<Integer>();
	for (final ValueExpression ve : veList)
	    if (ve instanceof ColumnReference) {
		final ColumnReference cr = (ColumnReference) ve;
		indexes.add(cr.getColumnIndex());
		// }else if(ve instanceof IntegerYearFromDate){
		// //SUPPORT FOR EXTRACT_YEAR
		// IntegerYearFromDate iyfd = (IntegerYearFromDate) ve;
		// ColumnReference<Date> veDate = (ColumnReference<Date>)
		// iyfd.getInnerExpressions().get(0);
		// indexes.add(veDate.getColumnIndex());
	    } else
		throw new RuntimeException(
			"Should check with isAllColumnReferences before use!");
	return indexes;
    }

    public static String generateUniqueName(String nameBase) {
	if (_uniqueNumbers.containsKey(nameBase)) {
	    final int available = _uniqueNumbers.remove(nameBase);
	    _uniqueNumbers.put(nameBase, available + 1);
	    return nameBase + available;
	} else {
	    _uniqueNumbers.put(nameBase, 1);
	    return nameBase + "0";
	}
    }

    public static List<ColumnReference> getColumnRefFromVEs(
	    List<ValueExpression> veList) {
	final List<ColumnReference> crList = new ArrayList<ColumnReference>();
	for (final ValueExpression ve : veList) {
	    final ColumnRefCollectVisitor colVisitor = new ColumnRefCollectVisitor();
	    ve.accept(colVisitor);
	    crList.addAll(colVisitor.getColumnRefs());
	}
	return crList;
    }

    public static List<Integer> getColumnRefIndexes(List<ColumnReference> crList) {
	final List<Integer> intList = new ArrayList<Integer>();
	for (final ColumnReference cr : crList)
	    intList.add(cr.getColumnIndex());
	return intList;
    }

    public static List<String> getCompNamesFromColumns(List<Column> columns) {
	final List<String> compNameList = new ArrayList<String>();
	for (final Column column : columns) {
	    final String compName = getComponentName(column);
	    compNameList.add(compName);
	}
	return compNameList;
    }

    /*
     * Find component in a query with a given name
     */
    public static Component getComponent(String compName, CompGen cg) {
	return cg.getQueryBuilder().getComponent(compName);
    }

    public static String getComponentName(Column column) {
	return getComponentName(column.getTable());
    }

    /*
     * We assume all the columns refer to the same DataSourceComponent
     */
    public static String getComponentName(List<Column> columns) {
	// all the columns will be from the same table, so we can choose any of
	// them
	// (here we choose the first one)
	final Column column = columns.get(0);
	return getComponentName(column);
    }

    public static String getComponentName(Table table) {
	String tableName = table.getAlias();
	if (tableName == null)
	    tableName = table.getName();
	return tableName;
    }

    /*
     * Find a list of components in a query with a given list of names
     */
    public static List<Component> getComponents(List<String> compNameList,
	    CompGen cg) {
	final List<Component> compList = new ArrayList<Component>();
	for (final String compName : compNameList)
	    compList.add(getComponent(compName, cg));
	return compList;
    }

    public static <T> List<T> getDifference(List<T> bigger, List<T> smaller) {
	final List<T> result = new ArrayList<T>();
	for (final T elem1 : bigger)
	    if (!smaller.contains(elem1))
		result.add(elem1);
	return result;
    }

    /*
     * returns N1.NATIONNAME
     */
    public static String getFullAliasedName(Column column) {
	return getFullName(getComponentName(column), column.getColumnName());
    }

    public static String getFullName(String tableCompName, String columnName) {
	return tableCompName + "." + columnName;
    }

    public static String getFullSchemaColumnName(Column column,
	    TableAliasName tan) {
	final String tableCompName = getComponentName(column);
	final String tableSchemaName = tan.getSchemaName(tableCompName);
	final String columnName = column.getColumnName();
	return getFullName(tableSchemaName, columnName);
    }

    public static String getFullSchemaColumnName(String fullAliasedName,
	    TableAliasName tan) {
	final String[] parts = fullAliasedName.split("\\.");
	final String tableCompName = parts[0];
	final String columnName = parts[1];
	return getFullName(tan.getSchemaName(tableCompName), columnName);
    }

    /*
     * The result will have duplicates only if there are duplicates in list1
     */
    public static <T> List<T> getIntersection(List<T> list1, List<T> list2) {
	final List<T> result = new ArrayList<T>();
	for (final T elem1 : list1)
	    if (list2.contains(elem1))
		result.add(elem1);
	return result;
    }

    public static List<Expression> getJoinCondition(SQLVisitor pq,
	    Component left, Component right) {
	final List<String> leftAncestors = getSourceNameList(left);
	final List<String> rightAncestors = getSourceNameList(right);
	return pq.getJte().getExpressions(leftAncestors, rightAncestors);
    }

    public static List<Column> getJSQLColumns(Expression expr) {
	final ColumnCollectVisitor columnCollect = new ColumnCollectVisitor();
	expr.accept(columnCollect);
	return columnCollect.getColumns();
    }

    public static List<Column> getJSQLColumns(List<Expression> exprs) {
	final List<Column> result = new ArrayList<Column>();
	for (final Expression expr : exprs)
	    result.addAll(getJSQLColumns(expr));
	return result;
    }

    /*
     * get the number of elements with the value in a segment [0, endIndex).
     * Used to get the number of hashIndexes(elements) which are smaller than
     * index(endIndex).
     */
    public static int getNumElementsBefore(int endIndex, List<Integer> elements) {
	int numBefore = 0;
	for (int i = 0; i < endIndex; i++)
	    if (elements.contains(i))
		numBefore++;
	return numBefore;
    }

    public static int getPreOpsOutputSize(Component component, Schema schema,
	    TableAliasName tan) {
	if (component instanceof ThetaJoinComponent)
	    throw new RuntimeException(
		    "SQL generator with Theta does not work for now!");
	// TODO similar to Equijoin, but not subtracting joinColumnsLength

	final Component[] parents = component.getParents();
	if (parents == null)
	    // this is a DataSourceComponent
	    return getPreOpsOutputSize((DataSourceComponent) component, schema,
		    tan);
	else if (parents.length == 1)
	    return getPreOpsOutputSize(parents[0], schema, tan);
	else if (parents.length == 2) {
	    final Component firstParent = parents[0];
	    final Component secondParent = parents[1];
	    final int joinColumnsLength = firstParent.getHashIndexes().size();
	    return getPreOpsOutputSize(firstParent, schema, tan)
		    + getPreOpsOutputSize(secondParent, schema, tan)
		    - joinColumnsLength;
	}
	throw new RuntimeException("More than two parents for a component "
		+ component);
    }

    /*
     * Used in Simple and Rule optimizer Used when index of a column has to be
     * obtained before EarlyProjection is performed.
     */
    public static int getPreOpsOutputSize(DataSourceComponent source,
	    Schema schema, TableAliasName tan) {
	final String tableSchemaName = tan.getSchemaName(source.getName());
	return schema.getTableSchema(tableSchemaName).size();
    }

    public static List<ColumnNameType> getProjectedSchema(
	    List<ColumnNameType> schema, List<Integer> hashIndexes) {
	final List<ColumnNameType> result = new ArrayList<ColumnNameType>();
	for (final Integer hashIndex : hashIndexes)
	    // the order is improtant
	    result.add(schema.get(hashIndex));
	return result;
    }

    public static List<String> getSourceNameList(Component component) {
	final List<DataSourceComponent> sources = component
		.getAncestorDataSources();
	final List<String> compNames = new ArrayList<String>();
	for (final DataSourceComponent source : sources)
	    compNames.add(source.getName());
	return compNames;
    }

    public static Set<String> getSourceNameSet(Component component) {
	final List<DataSourceComponent> sources = component
		.getAncestorDataSources();
	final Set<String> compNames = new HashSet<String>();
	for (final DataSourceComponent source : sources)
	    compNames.add(source.getName());
	return compNames;
    }

    // We couldn't change toString methods without invasion to JSQL classes
    public static String getStringExpr(Expression expr) {
	final PrintVisitor printer = new PrintVisitor();
	expr.accept(printer);
	return printer.getString();
    }

    public static String getStringExpr(ExpressionList params) {
	final PrintVisitor printer = new PrintVisitor();
	params.accept(printer);
	return printer.getString();
    }

    // we use this method for List<OrExpression> as well
    public static <T extends Expression> String getStringExpr(List<T> listExpr) {
	final StringBuilder sb = new StringBuilder();
	final int size = listExpr.size();
	for (int i = 0; i < size; i++) {
	    sb.append(getStringExpr(listExpr.get(i)));
	    if (i != size - 1)
		// not the last element
		sb.append(", ");
	}
	return sb.toString();
    }

    public static List<Expression> getSubExpressions(Expression expr) {
	final List<Expression> result = new ArrayList<Expression>();
	if (expr instanceof BinaryExpression) {
	    final BinaryExpression be = (BinaryExpression) expr;
	    result.add(be.getLeftExpression());
	    result.add(be.getRightExpression());
	} else if (expr instanceof Parenthesis) {
	    final Parenthesis prnths = (Parenthesis) expr;
	    result.add(prnths.getExpression());
	} else if (expr instanceof Function) {
	    final Function fun = (Function) expr;
	    final ExpressionList params = fun.getParameters();
	    if (params != null)
		result.addAll(params.getExpressions());
	} else
	    return null;
	return result;
    }

    // can be used *before* parallelismToMap method is invoked
    public static int getTotalParallelism(NameCompGen ncg) {
	int totalParallelism = 0;
	final Map<String, CostParams> compCost = ncg.getCompCost();
	for (final Map.Entry<String, CostParams> compNameCost : compCost
		.entrySet())
	    totalParallelism += compNameCost.getValue().getParallelism();
	return totalParallelism;
    }

    // can be used *after* parallelismToMap method is invoked
    public static int getTotalParallelism(QueryBuilder plan,
	    Map<String, String> map) {
	int totalParallelism = 0;

	for (final String compName : plan.getComponentNames()) {
	    final String parallelismStr = SystemParameters.getString(map,
		    compName + "_PAR");
	    totalParallelism += Integer.valueOf(parallelismStr);
	}

	return totalParallelism;
    }

    public static boolean isAllColumnRefs(List<ValueExpression> veList) {
	for (final ValueExpression ve : veList)
	    if (!(ve instanceof ColumnReference))
		return false;
	return true;
    }

    /*
     * is joinComponent the last component in the query plan, in terms of no
     * more joins to perform
     */
    public static boolean isFinalComponent(Component comp, SQLVisitor pq) {
	final Set<String> allSources = new HashSet<String>(pq.getTan()
		.getComponentNames());
	final Set<String> actuallPlanSources = getSourceNameSet(comp);
	return allSources.equals(actuallPlanSources);
    }

    public static boolean isSameSchema(TupleSchema listSchema1,
	    TupleSchema listSchema2) {
	final Set<ColumnNameType> setSchema1 = new HashSet<ColumnNameType>(
		listSchema1.getSchema());
	final Set<ColumnNameType> setSchema2 = new HashSet<ColumnNameType>(
		listSchema2.getSchema());
	return setSchema1.equals(setSchema2);
    }

    // throw away join hash indexes from the right parent
    public static TupleSchema joinSchema(Component[] parents,
	    Map<String, CostParams> compCost) {
	final Component leftParent = parents[0];
	final Component rightParent = parents[1];
	final TupleSchema leftSchema = compCost.get(leftParent.getName())
		.getSchema();
	final TupleSchema rightSchema = compCost.get(rightParent.getName())
		.getSchema();
	final List<ColumnNameType> leftCnts = leftSchema.getSchema();
	final List<ColumnNameType> rightCnts = rightSchema.getSchema();

	// when HashExpressions are used the schema is a simple appending
	final List<Integer> leftHashIndexes = leftParent.getHashIndexes();
	final List<Integer> rightHashIndexes = rightParent.getHashIndexes();

	// ******************** similar to MyUtilities.createOutputTuple
	final List<ColumnNameType> outputSchema = new ArrayList<ColumnNameType>();

	for (int i = 0; i < leftCnts.size(); i++)
	    // first relation (R)
	    outputSchema.add(leftCnts.get(i));
	for (int i = 0; i < rightCnts.size(); i++)
	    if ((rightHashIndexes == null) || (!rightHashIndexes.contains(i)))
		// does
		// not
		// exits
		// add
		// the
		// column!!
		// (S)
		outputSchema.add(rightCnts.get(i));
	final TupleSchema result = new TupleSchema(outputSchema);
	// ******************** end of similar

	// creating a list of synonyms - TODO: works only for left-deep plans
	final Map<String, String> synonims = new HashMap<String, String>();
	final Map<String, String> leftSynonims = leftSchema.getSynonims();
	if (leftSynonims != null)
	    synonims.putAll(leftSynonims); // have to add all the synonyms from
	// left parent
	for (int i = 0; i < rightHashIndexes.size(); i++) {
	    final int leftHash = leftHashIndexes.get(i);
	    final int rightHash = rightHashIndexes.get(i);
	    final String rightColStr = rightCnts.get(rightHash).getName();
	    final String originalColumnStr = leftCnts.get(leftHash).getName();
	    synonims.put(rightColStr, originalColumnStr);
	}
	if (!synonims.isEmpty())
	    result.setSynonims(synonims);

	return result;
    }

    public static int[] listToArr(List<Integer> list) {
	final int[] arr = new int[list.size()];
	for (int i = 0; i < list.size(); i++)
	    arr[i] = list.get(i);
	return arr;
    }

    public static <T> String listToStr(List<T> list) {
	final StringBuilder sb = new StringBuilder();
	sb.append("(");
	for (int i = 0; i < list.size(); i++) {
	    sb.append(list.get(i));
	    if (i == list.size() - 1)
		sb.append(")");
	    else
		sb.append(", ");
	}
	return sb.toString();
    }

    public static Column nameToColumn(String name) {
	final String[] nameParts = name.split("\\.");
	final String compName = nameParts[0];
	final String columnName = nameParts[1];

	final Table table = new Table();
	table.setAlias(compName);
	final Column column = new Column();
	column.setColumnName(columnName);
	column.setTable(table);
	return column;
    }

    private static List<Operator> orderOperators(ChainOperator chain) {
	final List<Operator> result = new ArrayList<Operator>();

	final Operator selection = chain.getSelection();
	if (selection != null)
	    result.add(selection);

	final Operator distinct = chain.getDistinct();
	if (distinct != null)
	    result.add(distinct);

	final Operator projection = chain.getProjection();
	if (projection != null)
	    result.add(projection);

	final Operator agg = chain.getAggregation();
	if (agg != null)
	    result.add(agg);

	return result;
    }

    /*
     * On each component order the Operators as Select, Distinct, Project,
     * Aggregation
     */
    public static void orderOperators(QueryBuilder queryPlan) {
	final List<Component> comps = queryPlan.getPlan();
	for (final Component comp : comps) {
	    final ChainOperator chain = comp.getChainOperator();
	    chain.setOperators(orderOperators(chain));
	}
    }

    public static int parallelismToMap(Map<String, Integer> compNamePars,
	    Map map) {
	int totalParallelism = 0;
	for (final Map.Entry<String, Integer> compNamePar : compNamePars
		.entrySet()) {
	    final String compName = compNamePar.getKey();
	    final int parallelism = compNamePar.getValue();

	    if (parallelism == 0)
		throw new RuntimeException("Unset parallelism for component "
			+ compName + " !");
	    else if (parallelism < 0)
		throw new RuntimeException(
			"Negative parallelism for component " + compName + " !");

	    totalParallelism += parallelism;
	    SystemParameters.putInMap(map, compName + "_PAR", parallelism);
	}
	return totalParallelism;
    }

    public static int parallelismToMap(NameCompGen cg, Map map) {
	final Map<String, Integer> compNamePars = new HashMap<String, Integer>();
	for (final Component comp : cg.getQueryBuilder().getPlan()) {
	    final String compName = comp.getName();
	    final int parallelism = cg.getCostParameters(compName)
		    .getParallelism();
	    compNamePars.put(compName, parallelism);
	}
	return parallelismToMap(compNamePars, map);
    }

    public static SQLVisitor parseQuery(Map map) {
	final String sqlString = readSQL(map);

	final CCJSqlParserManager pm = new CCJSqlParserManager();
	Statement statement = null;
	try {
	    statement = pm.parse(new StringReader(sqlString));
	} catch (final JSQLParserException ex) {
	    LOG.info("JSQLParserException");
	}

	if (statement instanceof Select) {
	    final Select selectStatement = (Select) statement;
	    final String queryName = SystemParameters.getString(map,
		    "DIP_QUERY_NAME");
	    final SQLVisitor parsedQuery = new SQLVisitor(queryName);

	    // visit whole SELECT statement
	    parsedQuery.visit(selectStatement);
	    parsedQuery.doneVisiting();

	    return parsedQuery;
	}
	throw new RuntimeException("Please provide SELECT statement!");
    }

    // used after parallelismToMap method is invoked
    public static String parToString(QueryBuilder plan, Map<String, String> map) {
	int totalParallelism = 0;

	final StringBuilder sb = new StringBuilder("\n\nPARALLELISM:\n");
	for (final String compName : plan.getComponentNames()) {
	    final String parallelismStr = SystemParameters.getString(map,
		    compName + "_PAR");
	    sb.append(compName).append(" = ").append(parallelismStr)
		    .append("\n");

	    final int parallelism = Integer.valueOf(parallelismStr);
	    totalParallelism += parallelism;
	}
	sb.append("END OF PARALLELISM\n");

	sb.append("Total parallelism is ").append(totalParallelism)
		.append("\n\n");
	return sb.toString();
    }

    /*
     * JSQL printing
     */
    public static void printParsedQuery(SQLVisitor pq) {
	for (final Table table : pq.getTableList()) {
	    final String tableStr = toString(table);
	    LOG.info(tableStr);
	}
    }

    private static String readSQL(Map map) {
	final String queryName = SystemParameters.getString(map,
		"DIP_QUERY_NAME");
	final String sqlPath = SystemParameters.getString(map, "DIP_SQL_ROOT")
		+ queryName + SQL_EXTENSION;
	return MyUtilities.readFileSkipEmptyAndComments(sqlPath);
    }

    public static String toString(Join join) {
	final StringBuilder joinSB = new StringBuilder();

	joinSB.append("Join: my right table is ").append(join.getRightItem())
		.append(".");

	String type = "";
	if (join.isFull())
	    type += "Full ";
	if (join.isInner())
	    type += " Inner";
	if (join.isLeft())
	    type += " Left";
	if (join.isNatural())
	    type += " Natural";
	if (join.isOuter())
	    type += " Outer";
	if (join.isRight())
	    type += " Right";
	if (join.isSimple())
	    type += " Simple";
	joinSB.append("\nThe join type(s): ").append(type).append(".");
	joinSB.append("\nThe join condition(s): ")
		.append(join.getOnExpression()).append(".\n");

	final String joinStr = joinSB.toString();
	return joinStr;
    }

    /*
     * Squall query plan printing - Indexes
     */
    public static String toString(QueryBuilder queryPlan) {
	final StringBuilder sb = new StringBuilder("QUERY PLAN");
	for (final Component comp : queryPlan.getPlan()) {
	    sb.append("\n\nComponent ").append(comp.getName());

	    final ChainOperator chain = comp.getChainOperator();
	    if (!chain.isEmpty())
		sb.append("\n").append(chain);

	    if (comp.getHashIndexes() != null
		    && !comp.getHashIndexes().isEmpty())
		sb.append("\n HashIndexes: ").append(
			listToStr(comp.getHashIndexes()));
	    if (comp.getHashExpressions() != null
		    && !comp.getHashExpressions().isEmpty())
		sb.append("\n HashExpressions: ").append(
			listToStr(comp.getHashExpressions()));
	}
	sb.append("\n\nEND of QUERY PLAN");
	return sb.toString();
    }

    public static String toString(Table table) {
	return table.getWholeTableName();
    }

    private static Logger LOG = Logger.getLogger(ParserUtil.class);

    public static final int NOT_FOUND = -1;

    private final static String SQL_EXTENSION = ".sql";

    private static HashMap<String, Integer> _uniqueNumbers = new HashMap<String, Integer>();

}