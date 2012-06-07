/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package estimators;

import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import schema.Schema;
import schema.TPCH_Schema;
import util.ParserUtil;
import util.TableAliasName;


/* TODO high prio:
 * do not work when invoked on something which is not DataSourceComponent (no exception)
 * AndExpression (exception) - has to work for all the components, look at the textbook
 *
 * TODO low prio:
 * assume R.A < 4, column is always on the left (exception)
 * do not support R.A + R.B = 2 or R.A + 4 = 2 (no exception)
 */
public class SelingerSelectivityEstimator implements SelectivityEstimator{

    private Schema _schema;
    private TableAliasName _tan;
    private JSQLTypeConverter _converter = new JSQLTypeConverter();

    public SelingerSelectivityEstimator(Schema schema, TableAliasName tan){
        _schema = schema;
        _tan = tan;
    }

    public double estimate(Expression expr) {
        throw new RuntimeException("We should be in a more specific method!");
    }

    public double estimate(EqualsTo equals){
        List<Column> columns = ParserUtil.invokeColumnVisitor(equals);
        Column column = columns.get(0);
        String fullSchemaColumnName = _tan.getFullSchemaColumnName(column);

        int distinctValues = _schema.getNumDistinctValues(fullSchemaColumnName);
        return 1.0/distinctValues;
    }


    public double estimate(MinorThan mt){
        List<Column> columns = ParserUtil.invokeColumnVisitor(mt);
        Column column = columns.get(0);
        String fullSchemaColumnName = _tan.getFullSchemaColumnName(column);
        TypeConversion tc = _schema.getType(fullSchemaColumnName);

        //assume uniform distribution
        Object minValue = _schema.getRange(fullSchemaColumnName).getMin();
        Object maxValue = _schema.getRange(fullSchemaColumnName).getMax();
        double fullRange = tc.getDistance(maxValue, minValue);

        Object currentValue = _converter.convert(mt.getRightExpression());
        double distance = tc.getDistance(currentValue, minValue);

        return distance/fullRange;
    }

    /*
     * computed using the basic ones (= and <)
     */
    public double estimate(NotEqualsTo ne){
        EqualsTo equals = new EqualsTo();
        equals.setLeftExpression(ne.getLeftExpression());
        equals.setRightExpression(ne.getRightExpression());

        return 1 - estimate(equals);
    }

    public double estimate(GreaterThanEquals gt){
        MinorThan minorThan = new MinorThan();
        minorThan.setLeftExpression(gt.getLeftExpression());
        minorThan.setRightExpression(gt.getRightExpression());

        return 1 - estimate(minorThan);
    }

    public double estimate(GreaterThan gt){
        EqualsTo equals = new EqualsTo();
        equals.setLeftExpression(gt.getLeftExpression());
        equals.setRightExpression(gt.getRightExpression());

        MinorThan minorThan = new MinorThan();
        minorThan.setLeftExpression(gt.getLeftExpression());
        minorThan.setRightExpression(gt.getRightExpression());

        return 1 - estimate(equals) - estimate(minorThan);
    }

    public double estimate(MinorThanEquals mte){
        EqualsTo equals = new EqualsTo();
        equals.setLeftExpression(mte.getLeftExpression());
        equals.setRightExpression(mte.getRightExpression());

        MinorThan minorThan = new MinorThan();
        minorThan.setLeftExpression(mte.getLeftExpression());
        minorThan.setRightExpression(mte.getRightExpression());

        return estimate(minorThan) + estimate(equals);
    }

    /*
     * And, Or expressions
     */
    public double estimate(OrExpression or){
        return estimate(or.getLeftExpression()) + estimate(or.getRightExpression());
    }

    public double estimate(AndExpression and){
        throw new RuntimeException("Not having a good way of doing so for now!");
    }

    //just for testing purposes
    public static void main(String[] args){

        Table table = new Table();
        table.setName("ORDERS");
        List<Table> tableList = new ArrayList<Table>(Arrays.asList(table));

        Column column = new Column();
        column.setTable(table);
        column.setColumnName("ORDERDATE");

        MinorThan mt = new MinorThan();
        mt.setLeftExpression(column);
        mt.setRightExpression(new DateValue("d" + "1995-01-01" + "d"));
        
        SelingerSelectivityEstimator selEstimator = new SelingerSelectivityEstimator(new TPCH_Schema(), new TableAliasName(tableList));
        System.out.println(selEstimator.estimate(mt)); 
    }


}