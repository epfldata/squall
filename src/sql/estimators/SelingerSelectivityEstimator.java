package sql.estimators;

import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import plan_runner.conversion.TypeConversion;
import sql.schema.Schema;
import sql.util.ParserUtil;
import sql.util.TableAliasName;


/* TODO high prio:
 * no matter on which component we do invoke, the only important is to know previous projections
 *   (TPCH7, 8 with pushing OR from WHERE clause)
 *
 * TODO low prio:
 * do not support R.A + 4 < 2, we will need ValueExpression with ranges to support that, and a user can rewrite it itself
 * do not support R.A + R.B = 2 (no exception)
 *   if there are multiple fields addressed, they have probably some dependency, which we don't model yet
 */
public class SelingerSelectivityEstimator implements SelectivityEstimator{

    private Schema _schema;
    private TableAliasName _tan;

    public SelingerSelectivityEstimator(Schema schema, TableAliasName tan){
        _schema = schema;
        _tan = tan;
    }

    public double estimate(List<Expression> exprs){
        //this is treated as a list of AndExpressions
        if(exprs.size() == 1){
            return estimate(exprs.get(0));
        }

        //at least two expressions in the list
        AndExpression and = new AndExpression(exprs.get(0), exprs.get(1));
        for(int i = 2; i < exprs.size(); i++){
            and = new AndExpression(and, exprs.get(i));
        }
        return estimate(and);
    }

    public double estimate(Expression expr) {
        //  similarly to JSQLTypeConvertor, it can be done via visitor pattern, 
        //  but then it cannot implement SelectivityEstimator anymore.
        //the gap between void of visit method and double as the result here 
        //  can be solved in a similar manner as in JSQLTypeConverter.
        if(expr instanceof EqualsTo){
            return estimate((EqualsTo)expr);
        }else if(expr instanceof NotEqualsTo){
            return estimate((NotEqualsTo)expr);
        }else if(expr instanceof MinorThan){
            return estimate((MinorThan)expr);
        }else if(expr instanceof MinorThanEquals){
            return estimate((MinorThanEquals)expr);
        }else if(expr instanceof GreaterThan){
            return estimate((GreaterThan)expr);
        }else if(expr instanceof GreaterThanEquals){
            return estimate((GreaterThanEquals)expr);
        }else if(expr instanceof AndExpression){
            return estimate((AndExpression)expr);
        }else if(expr instanceof OrExpression){
            return estimate((OrExpression)expr);
        }else if(expr instanceof Parenthesis){
            Parenthesis pnths = (Parenthesis) expr;
            return estimate(pnths.getExpression());
        }else if(expr instanceof LikeExpression){
            //TODO: this is for TPCH9
            return 0.052;
        }
        throw new RuntimeException("We should be in a more specific method!");
    }

    public double estimate(EqualsTo equals){
        List<Column> columns = ParserUtil.getJSQLColumns(equals);

        Column column = columns.get(0);
        String fullSchemaColumnName = _tan.getFullSchemaColumnName(column);

        long distinctValues = _schema.getNumDistinctValues(fullSchemaColumnName);
        return 1.0/distinctValues;
    }


    public double estimate(MinorThan mt){
        List<Column> columns = ParserUtil.getJSQLColumns(mt);
        Column column = columns.get(0);

        TypeConversion tc = _schema.getType(ParserUtil.getFullSchemaColumnName(column, _tan));

        //assume uniform distribution
        String fullSchemaColumnName = _tan.getFullSchemaColumnName(column);
        Object minValue = _schema.getRange(fullSchemaColumnName).getMin();
        Object maxValue = _schema.getRange(fullSchemaColumnName).getMax();
        double fullRange = tc.getDistance(maxValue, minValue);

        //on one of the sides we have to have a constant
        JSQLTypeConverter rightConverter = new JSQLTypeConverter();
        mt.getRightExpression().accept(rightConverter);
        Object currentValue = rightConverter.getResult();
        if(currentValue == null){
            JSQLTypeConverter leftConverter = new JSQLTypeConverter();
            mt.getLeftExpression().accept(leftConverter);
            currentValue = leftConverter.getResult();
        }
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
        //the case when we have the same single column on both sides
        Expression leftExpr = and.getLeftExpression();
        List<Column> leftColumns = ParserUtil.getJSQLColumns(leftExpr);
        Column leftColumn = leftColumns.get(0);

        Expression rightExpr = and.getRightExpression();
        List<Column> rightColumns = ParserUtil.getJSQLColumns(rightExpr);
        Column rightColumn = rightColumns.get(0);

        if(leftColumn.toString().equals(rightColumn.toString())){
            //not using leftExpr and rightExpr, because we want to preserve type
            return 1 - (1 - estimate(and.getLeftExpression())) - (1 - estimate(and.getRightExpression()));
        }else{
            return estimate(and.getLeftExpression()) * estimate(and.getRightExpression());
        }

    }

}