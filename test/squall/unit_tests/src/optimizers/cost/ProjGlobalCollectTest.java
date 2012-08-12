package optimizers.cost;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import sql.main.ParserMain;
import sql.optimizers.name.ProjGlobalCollect;
import sql.util.ParserUtil;
import sql.visitors.jsql.SQLVisitor;

/**
 *
 * @author vitorovi
 */
public class ProjGlobalCollectTest {
    
    @Test
    public void testEverything() {
        System.out.println("test ProjGlobalCollect");
        
        //create object
        String parserConfPath = "../test/squall/unit_tests/confs/0.1G_tpch7_ncl_serial";
        ParserMain pm = new ParserMain();
        Map map = pm.createConfig(parserConfPath);
        SQLVisitor parsedQuery = ParserUtil.parseQuery(map);
        ProjGlobalCollect instance = new ProjGlobalCollect(parsedQuery.getSelectItems(), parsedQuery.getWhereExpr());
        instance.process();
        
        //getting results
        List<Expression> listExpr = instance.getExprList();
        List<OrExpression> orListExpr = instance.getOrExprs();
                
        //expected results
        Table tableN1 = new Table(); tableN1.setName("N1");
        Table tableN2 = new Table(); tableN2.setName("N2");
        Table tableLineitem = new Table(); tableLineitem.setName("LINEITEM");
        
        Column se1 = new Column(); se1.setTable(tableN1); se1.setColumnName("NAME");
        Column se2 = new Column(); se2.setTable(tableN2); se2.setColumnName("NAME");
        Column ls = new Column(); ls.setTable(tableLineitem); ls.setColumnName("SHIPDATE");
        ExpressionList el = new ExpressionList();
        el.setExpressions(Arrays.asList(ls));
        Function se3 = new Function(); se3.setName("EXTRACT_YEAR"); se3.setParameters(el);
        Column le = new Column(); le.setTable(tableLineitem); le.setColumnName("EXTENDEDPRICE");
        Column ld = new Column(); ld.setTable(tableLineitem); ld.setColumnName("DISCOUNT");
        Subtraction diff = new Subtraction(); diff.setLeftExpression(new DoubleValue("1.0")); diff.setRightExpression(ld);
        Parenthesis diffPnths = new Parenthesis();diffPnths.setExpression(diff);
        Multiplication se4 = new Multiplication(); se4.setLeftExpression(le); se4.setRightExpression(diffPnths);
        List<Expression> expListExpr = Arrays.asList(se1, se2, se3, se4);
        
        
        EqualsTo eq1 = new EqualsTo(); eq1.setLeftExpression(se1); eq1.setRightExpression(new StringValue(" FRANCE "));
        EqualsTo eq2 = new EqualsTo(); eq2.setLeftExpression(se2); eq2.setRightExpression(new StringValue(" GERMANY "));
        AndExpression and1 = new AndExpression(eq1, eq2); 
        Parenthesis p1 = new Parenthesis(); p1.setExpression(and1);
        EqualsTo eq3 = new EqualsTo(); eq3.setLeftExpression(se1); eq3.setRightExpression(new StringValue(" GERMANY "));
        EqualsTo eq4 = new EqualsTo(); eq4.setLeftExpression(se2); eq4.setRightExpression(new StringValue(" FRANCE "));
        AndExpression and2 = new AndExpression(eq3, eq4); 
        Parenthesis p2 = new Parenthesis(); p2.setExpression(and2);
        List<OrExpression> expOrListExpr = Arrays.asList(new OrExpression(p1, p2));
        
        //compare its string representation: computed are first two, expected are second two
        String strListExpr = ParserUtil.getStringExpr(listExpr);
        String strOrListExpr = ParserUtil.getStringExpr(orListExpr);
        String strExpListExpr = ParserUtil.getStringExpr(expListExpr);
        String strExpOrListExpr = ParserUtil.getStringExpr(expOrListExpr);
        
        assertEquals(strExpListExpr, strListExpr);
        assertEquals(strExpOrListExpr, strOrListExpr);
    }
   
}