/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package operators;

import storage.AggStorage;
import expressions.ValueExpression;
import java.util.List;


public interface AggregateOperator<T> extends Operator{
    // GROUP BY ValueExpression is not part of the SQL standard, only columns can be sed.
    public AggregateOperator setGroupByColumns(List<Integer> groupByColumns);
    public List<Integer> getGroupByColumns();
    public AggregateOperator setGroupByProjection(ProjectOperator projection);
    public ProjectOperator getGroupByProjection();
    
    //SUM(DISTINCT ValueExpression), COUNT(DISTINCT ValueExpression): a single ValueExpression by SQL standard
    //  MySQL supports multiple ValueExpression. Inside aggregation(SUM, COUNT), there must be single ValueExpression.
    public AggregateOperator setDistinct(DistinctOperator distinct);
    public DistinctOperator getDistinct();

    //this is null for AggregateCountOperator
    public List<ValueExpression> getExpressions();

    //internal storage class
    public AggStorage getStorage();
    public void clearStorage();

    public T runAggregateFunction(T value, List<String> tuple);
    public T runAggregateFunction(T value1, T value2);

    //HAVING clause: Since HAVING depends on aggregate result,
    //  it cannot be evaluated before all the tuples are processed.
    //This will be done by user manually, as well as ORDER BY clause.

    //COUNT(COLUMN_NAME): an AggregateCountOperator *preceeded* by a SelectionOperator.
    //  this counts non-null COLUMN_NAMEs
    //  due to the order of operators, this is not doable in our system

}
